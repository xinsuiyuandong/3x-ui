package service

import (
	"encoding/json"
	"errors"
	"runtime"
	"sync"
    "strconv"

	"x-ui/logger"
	"x-ui/xray"
	json_util "x-ui/util/json_util"

	"go.uber.org/atomic"
)

var (
	p                 *xray.Process
	lock              sync.Mutex
	isNeedXrayRestart atomic.Bool // Indicates that restart was requested for Xray
	isManuallyStopped atomic.Bool // Indicates that Xray was stopped manually from the panel
	result            string
)

type XrayService struct {
	inboundService InboundService
	settingService SettingService
	xrayAPI        xray.XrayAPI
}

// IsXrayRunning 检查 Xray 是否正在运行
func (s *XrayService) IsXrayRunning() bool {
	return p != nil && p.IsRunning()
}

// 中文注释:
// 新增 GetApiPort 函数。
// 这个函数的作用是安全地返回当前 Xray 进程正在监听的 API 端口号。
// 如果 Xray 没有运行 (p == nil)，则返回 0。
// 我们的后台任务将调用这个函数来获取端口号。
func (s *XrayService) GetApiPort() int {
	if p == nil {
		return 0
	}
	return p.GetAPIPort()
}


func (s *XrayService) GetXrayErr() error {
	if p == nil {
		return nil
	}

	err := p.GetErr()

	if runtime.GOOS == "windows" && err.Error() == "exit status 1" {
		// exit status 1 on Windows means that Xray process was killed
		// as we kill process to stop in on Windows, this is not an error
		return nil
	}

	return err
}

func (s *XrayService) GetXrayResult() string {
	if result != "" {
		return result
	}
	if s.IsXrayRunning() {
		return ""
	}
	if p == nil {
		return ""
	}

	result = p.GetResult()

	if runtime.GOOS == "windows" && result == "exit status 1" {
		// exit status 1 on Windows means that Xray process was killed
		// as we kill process to stop in on Windows, this is not an error
		return ""
	}

	return result
}

func (s *XrayService) GetXrayVersion() string {
	if p == nil {
		return "Unknown"
	}
	return p.GetVersion()
}

func RemoveIndex(s []any, index int) []any {
	return append(s[:index], s[index+1:]...)
}

func (s *XrayService) GetXrayConfig() (*xray.Config, error) {
	templateConfig, err := s.settingService.GetXrayConfigTemplate()
	if err != nil {
		return nil, err
	}

	xrayConfig := &xray.Config{}
	err = json.Unmarshal([]byte(templateConfig), xrayConfig)
	if err != nil {
		return nil, err
	}

	s.inboundService.AddTraffic(nil, nil)

	inbounds, err := s.inboundService.GetAllInbounds()
	if err != nil {
		return nil, err
	}

	// =================================================================
	// 中文注释: 动态限速核心逻辑 - 第一步: 收集所有限速值
	// =================================================================
	// 创建一个 map 用于存储所有出现过的、不为0的限速值
	uniqueSpeeds := make(map[int]bool)
	for _, inbound := range inbounds {
		if !inbound.Enable {
			continue
		}
		// 获取该入站下的所有客户端设置
		clients, _ := s.inboundService.GetClients(inbound)
		for _, client := range clients {
			if client.SpeedLimit > 0 {
				uniqueSpeeds[client.SpeedLimit] = true
			}
		}
	}

	// =================================================================
	// 中文注释: 动态限速核心逻辑 - 第二步: 根据收集到的限速值，动态生成 Policy Levels
	// =================================================================
	// 初始化 policy levels，并加入默认的 level 0 (不限速)
	policyLevels := make(map[string]interface{})
	policyLevels["0"] = map[string]interface{}{"handshake": 8, "connIdle": 500}

	// 遍历所有收集到的限速值
	for speed := range uniqueSpeeds {
		// 为每个速率创建一个 level，level 的名字就是速率的字符串形式
		// 例如，速率 1024 KB/s 对应 level "1024"
		policyLevels[strconv.Itoa(speed)] = map[string]interface{}{
			"downlinkOnly": speed, // 限制下载速度
			"uplinkOnly":   speed, // 同时限制上传速度 (您可以根据需要调整)
		}
	}

                // 将生成的 policy 应用到 Xray 配置中
                policyJSON, err := json.Marshal(map[string]interface{}{
                            "levels": policyLevels,
                         })
                   if err != nil {
                        return nil, err
                }
                xrayConfig.Policy = json_util.RawMessage(policyJSON)

	// =================================================================
    // 中文注释: 在这里增加日志，打印最终生成的限速策略
    // =================================================================
    if len(uniqueSpeeds) > 0 {
        finalPolicyLog, _ := json.Marshal(policyLevels)
        logger.Infof("已为Xray动态生成〔限速策略〕: %s", string(finalPolicyLog))
    }
    // =================================================================
	

	// =================================================================
	// 中文注释: 动态限速核心逻辑 - 第三步: 为设置了限速的用户分配对应的 Level
	// =================================================================
	for _, inbound := range inbounds {
		if !inbound.Enable {
			continue
		}
		// get settings clients
		settings := map[string]any{}
		json.Unmarshal([]byte(inbound.Settings), &settings)
		clients, ok := settings["clients"].([]any)
		if ok {
			// check users active or not
			clientStats := inbound.ClientStats
			for _, clientTraffic := range clientStats {
				indexDecrease := 0
				for index, client := range clients {
					c := client.(map[string]any)
					if c["email"] == clientTraffic.Email {
						if !clientTraffic.Enable {
							clients = RemoveIndex(clients, index-indexDecrease)
							indexDecrease++
							logger.Infof("Remove Inbound User %s due to expiration or traffic limit", c["email"])
						}
					}
				}
			}

			// clear client config for additional parameters
			var final_clients []any
			for _, client := range clients {
				c := client.(map[string]any)
				if enable, ok := c["enable"].(bool); ok && !enable { continue }

				// =================================================================
				// 这里的逻辑是准备将 client 对象提交给 Xray-core。
				// 我们需要将 speedLimit 转换为 Xray 认识的 level 字段。
				// 并且，我们不再删除任何字段，因为 Xray-core 会自动忽略它不认识的字段。
				// 这样可以确保包含 speedLimit 的完整用户信息被用于生成配置。
				// =================================================================
                if speedLimit, ok := c["speedLimit"].(float64); ok && speedLimit > 0 {
					c["level"] = int(speedLimit)
                    // 【新增功能】在这里添加日志记录
                    if email, emailOk := c["email"].(string); emailOk {
                        logger.Infof("为用户 %s 应用〔独立限速〕: %d KB/s", email, int(speedLimit))
                    }
				} else {
					c["level"] = 0
				}

				if c["flow"] == "xtls-rprx-vision-udp443" {
					c["flow"] = "xtls-rprx-vision"
				}

				final_clients = append(final_clients, c)
			}

			settings["clients"] = final_clients
			modifiedSettings, err := json.MarshalIndent(settings, "", "  ")
			if err != nil {
				return nil, err
			}

			inbound.Settings = string(modifiedSettings)
		}

		if len(inbound.StreamSettings) > 0 {
			// Unmarshal stream JSON
			var stream map[string]any
			json.Unmarshal([]byte(inbound.StreamSettings), &stream)

			// Remove the "settings" field under "tlsSettings" and "realitySettings"
			tlsSettings, ok1 := stream["tlsSettings"].(map[string]any)
			realitySettings, ok2 := stream["realitySettings"].(map[string]any)
			if ok1 || ok2 {
				if ok1 {
					delete(tlsSettings, "settings")
				} else if ok2 {
					delete(realitySettings, "settings")
				}
			}

			delete(stream, "externalProxy")

			newStream, err := json.MarshalIndent(stream, "", "  ")
			if err != nil {
				return nil, err
			}
			inbound.StreamSettings = string(newStream)
		}

		inboundConfig := inbound.GenXrayInboundConfig()
		xrayConfig.InboundConfigs = append(xrayConfig.InboundConfigs, *inboundConfig)
	}
	return xrayConfig, nil
}

func (s *XrayService) GetXrayTraffic() ([]*xray.Traffic, []*xray.ClientTraffic, error) {
	if !s.IsXrayRunning() {
		err := errors.New("xray is not running")
		logger.Debug("Attempted to fetch Xray traffic, but Xray is not running:", err)
		return nil, nil, err
	}
	apiPort := p.GetAPIPort()
	s.xrayAPI.Init(apiPort)
	defer s.xrayAPI.Close()

	traffic, clientTraffic, err := s.xrayAPI.GetTraffic(true)
	if err != nil {
		logger.Debug("Failed to fetch Xray traffic:", err)
		return nil, nil, err
	}
	return traffic, clientTraffic, nil
}

func (s *XrayService) RestartXray(isForce bool) error {
	lock.Lock()
	defer lock.Unlock()
	logger.Debug("restart Xray, force:", isForce)
	isManuallyStopped.Store(false)

	xrayConfig, err := s.GetXrayConfig()
	if err != nil {
		return err
	}

	// 【新功能】重启时，将完整配置打印到 Debug 日志以供验证
    configBytes, jsonErr := json.MarshalIndent(xrayConfig, "", "  ")
    if jsonErr == nil {
        logger.Debugf("使用新配置重启 Xray：\n%s", string(configBytes))
    } else {
        logger.Warning("无法将 Xray 配置编组以进行日志记录：", jsonErr)
    }

	if s.IsXrayRunning() {
		if !isForce && p.GetConfig().Equals(xrayConfig) && !isNeedXrayRestart.Load() {
			logger.Debug("It does not need to restart Xray")
			return nil
		}
		p.Stop()
	}

	p = xray.NewProcess(xrayConfig)
	result = ""
	err = p.Start()
	if err != nil {
		return err
	}

	return nil
}

func (s *XrayService) StopXray() error {
	lock.Lock()
	defer lock.Unlock()
	isManuallyStopped.Store(true)
	logger.Debug("Attempting to stop Xray...")
	if s.IsXrayRunning() {
		return p.Stop()
	}
	return errors.New("xray is not running")
}

func (s *XrayService) SetToNeedRestart() {
	isNeedXrayRestart.Store(true)
}

func (s *XrayService) IsNeedRestartAndSetFalse() bool {
	return isNeedXrayRestart.CompareAndSwap(true, false)
}

// Check if Xray is not running and wasn't stopped manually, i.e. crashed
func (s *XrayService) DidXrayCrash() bool {
	return !s.IsXrayRunning() && !isManuallyStopped.Load()
}
