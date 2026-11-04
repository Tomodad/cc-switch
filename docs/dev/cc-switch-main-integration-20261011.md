# CC Switch 最新 main 个性化集成：待验收候选与版本差异

## 基线与边界

本轮固定 **2026-10-11 23:11 UTC 获取的 upstream main 快照**，不滚动追逐后续提交，也不把它直接叫作纯 4.0.9 tag。

| 对照对象 | 固定提交 | 含义 |
| --- | --- | --- |
| 官方 4.0.4 tag | `a29a4f3868e9c6ffc2212957cfa66a718642e344` | 正式发布版本 |
| 旧定制 4.0.4 的实际官方基线 | `f9db9f7056cbe7f972cdc02644722002316866b9` | tag 后又有 6 个官方提交，已含 GitHub Copilot 托管账号与 Responses/Chat 路由 |
| 旧定制 4.0.4 归档 | `dc80a9f961d0224fa19350117dc07f94643ac472` | 旧验收安装来源，不能等同纯官方 4.0.4 |
| 官方 4.0.8 | `df40d53c36b604e044b4d5b9091aeb961aa56990` | 上一轮移植基线 |
| 4.0.8 定制候选 | `27c698093ae50d1358d72bbc15a26d8a672018dc` | 已按用户要求撤除私有缓存改动，保留工具权限限制 |
| 官方 4.0.9 tag | `8845b1ef799aa8810b864b122de6af5f4629de10` | 2026-10-11 15:29:48 UTC |
| **本轮官方 main 基线** | **`04bd15c9f86834c5e2acc46a973a7386994fb0bd`** | **2026-10-11 16:20:53 UTC；tag 后再多 2 个提交** |
| 本轮源码候选 | `d63d57d942f449fbd90d197e15e349eb63f4eefe` | 从上述 main 新建独立分支，不改旧归档、旧候选或 upstream PR |

本轮 main 相较 4.0.9 tag 额外包含 **NekoCode 预设**和**日文“详细选项”文案修正**。应用版本字段仍为 4.0.9；这是“4.0.9 发布后 main 快照 + 定制补丁”。

- [固定 main](https://github.com/farion1231/cc-switch/commit/04bd15c9f86834c5e2acc46a973a7386994fb0bd)
- [官方 4.0.9 发布提交](https://github.com/farion1231/cc-switch/commit/8845b1ef799aa8810b864b122de6af5f4629de10)
- [4.0.8 → 本轮 main 的官方差异](https://github.com/farion1231/cc-switch/compare/df40d53c36b604e044b4d5b9091aeb961aa56990...04bd15c9f86834c5e2acc46a973a7386994fb0bd)

## 相比 4.0.4，经过 4.0.8 已获得什么

以下是官方迭代带来的主要变化，不是重新实现的本地补丁。

1. **Codex 聚合和子代理更稳健。** 官方模型列表更可靠；只装 Codex Desktop、没有独立 CLI 时也能通过 Desktop 自带 CLI 获取模型。模型不可用提示能说明原因并在恢复后清除。聚合提供可选 classic v1 子代理工具；加密 v2 任务无法跨供应商读取时明确报错，不再把任务静默清空。v1 开关并不解密 v2 载荷，也不取代本地明文 delegation 规范化。
2. **会话、配置和账号连续性改善。** 支持读取压缩的会话 rollout 和压缩开关；压缩摘要回合保留历史所需工具定义，同时禁止新工具调用。修复顶层 base_url 保存、同用户不同 ChatGPT workspace 区分、托管账号删除解绑和旧客户端/重启提示。
3. **Windows 与平台体验改善。** 生成的 batch 避开非 ASCII 路径问题；支持更多 CLI/安装识别及 Linux 更新和窗口行为修复。这些并非本地 Windows Codex npm 平台包自愈的等效实现。
4. **用量与成本更准确。** Codex 缓存写入 token 导入、Claude 1 小时缓存费率、Sonnet/Haiku 5.5 定价、跨应用同名 provider 统计区分、全部历史热力图与配额展示改善。
5. **日常操作更方便。** 供应商搜索按钮及 API 地址搜索、Skills 批量更新确认、应用配置定位、诊断“打开日志”、拉取模型时使用自定义请求头、暗色文本降低眩光。

旧定制 4.0.4 已经基于 tag 后的 f9db9f70，所以 Copilot 托管支持不能算成本轮才获得的能力。

## 相比 4.0.8，本轮新增什么

1. **修复孤儿工具结果导致整个线程持续失败。** Chat 桥把找不到对应调用的 function/custom/tool_search 输出转成带标记的 user 消息，保持配对工具结果连续和媒体内容；xAI 原生 Responses 针对缺失 call_id 的 function_call_output 做类似处理。覆盖 Desktop create_thread、消息/自动化回执等常见失败形态。
2. **本地路由新增“连接地址”。** 客户端写入地址可与服务监听地址分离，帮助 WSL2 等无法访问宿主 loopback 的场景。配置只保存在设备本地；它不会自动改变防火墙或保证外部连通。
3. **模型选择对话框。** 获取模型后可搜索、勾选加入或取消删除、查看上下文/推理/图像参数；取消不修改。已有模型字段和顺序保留，新模型追加。Codex 模型列表前置，第一行就是默认模型。
4. **默认子代理模型与推理等级。** 普通 Codex 卡可写入 `[agents] default_subagent_model` 与 `default_subagent_reasoning_effort`，切换 provider 时一并切换；聚合模式不写这两个键，官方卡与 Grok Build 等不展示该配置。
5. **Claude 上游认证和聚合兼容性修正。** OpenAI 格式 Claude 上游使用 Bearer；Claude 聚合按成员合并工具搜索/实验 beta/Artifact 等兼容开关，降低选到非默认成员时的协议拒绝。
6. **体验和预设。** 冷启动正确应用保存语言、macOS 局域网权限声明、编辑器打开闪烁修复、表单解释收进帮助提示、多家新供应商预设；main 另含 NekoCode。

官方数据库 schema 仍为 20。新预设主要影响新建供应商，不自动改写既有卡片。

## 个性化补丁覆盖矩阵

结论：**这轮未发现任何完整补丁已被官方等效覆盖到可以整项撤除；delegation 从“未覆盖”变成“部分覆盖”。** 已撤除的私有缓存行为继续排除，不能因尚无官方等效实现而恢复。

| 本地能力 | 新官方覆盖情况 | 本轮处理 |
| --- | --- | --- |
| Managed 第三方 Chat/Anthropic 缺失 ToolSearch 声明补齐 | 官方仍只转换客户端已声明的 tool_search；不补缺失声明。PR #8120 尚未合并。 | 保留最小、请求域内 shim；官方/compact/覆盖工具配置等门控不扩大。 |
| 原生 Responses ToolSearch 往返 | 官方 passthrough 与 xAI namespace 扁平化没有完整搜索请求/历史/JSON/SSE 类型和 ID 恢复。 | 保留请求上下文、成功尝试来源、typed ID、JSON/SSE 恢复。 |
| 动态 schema、namespace 子工具、loaded subset | #7454 additional_tools 与 namespace registry 早已在官方中，但不等效于 schema alias、重复 namespace 合并、精确已加载子集。 | 使用官方已有基础，仅保留增量；原声明 schema 优先，未加载兄弟工具继续 deferred。 |
| 代理 catalog 的搜索能力 | 原生/Anthropic 直连保守 profile 仍是官方行为，未提供本地适配器可处理搜索时的单独代理 profile。 | 保留 direct/proxy 区分；保存的 tools/tool_choice override 不宣称额外能力。 |
| 已知模型 reasoning fallback / Responses Lite | 新 main 未在这些补丁区域提供等效行为。 | 仅保留旧已知模型回退；明确用户等级及官方/vendor 行不覆盖，不猜新模型。 |
| 明文 delegation 输入规范化 | **部分覆盖**：Chat orphan 与 xAI 无 call_id 修复处理同类回执；Anthropic/非 xAI native 不等效，严格 codex_app/tui 信封的原始任务语义也不同。 | 保留严格前置规范化；普通 orphan 完全交给官方新逻辑。新增两条组合回归，验证两者并存、none 不放宽、配对结果不改。 |
| Windows Codex npm 平台包自愈 | 官方 Windows repair 仍返回 None；batch 编码、Desktop CLI 发现是其他问题。 | 保留 bounded .cmd/.ps1 + 实际 sibling npm；不借 PATH，不处理 native/Volta/pnpm/缺失 sibling。 |
| history / namespace registry / #7454 / #6653 | 已由官方拥有，#7454/#6653 在官方 4.0.4 前已存在。 | 不重复搬旧实现。 |
| LevelDB / Statsig 私有 Desktop cache | 用户明确撤除，不以“官方未覆盖”为恢复理由。 | 不恢复任何读写、同步、续期、pin 或清理；公开 catalog 和 visibility 保留。 |
| `tool_choice: none` 自动改 auto | 会扩大工具权限，用户明确不保留。 | 继续禁止；allowed/forced 不扩大，无法表达的限制明确失败。 |
| 3.x WebSocket/诊断扩展 | 早在旧 4.0.4 集成已排除。 | 不恢复，保持当前官方 HTTP/SSE 边界。 |

官方新 orphan 核心函数已独立逐函数比对：Chat 输入遍历、单项转换、orphan flush、tool-result 分类，以及 xAI rewrite 均与固定 main **逐字相同**。三方应用无文本冲突并不替代这一语义审核。

关键证据：
- [官方 Chat orphan 修复 9055dd37](https://github.com/farion1231/cc-switch/commit/9055dd37bcd839f754d9941a62846321d4b648a4)
- [官方 xAI 修复 2b0ec81b](https://github.com/farion1231/cc-switch/commit/2b0ec81bb2f150ccfae7c60a462012a89c99f450)
- [最小 ToolSearch PR #8120](https://github.com/farion1231/cc-switch/pull/8120)
- [固定 main 完整 release notes](https://github.com/farion1231/cc-switch/blob/04bd15c9f86834c5e2acc46a973a7386994fb0bd/CHANGELOG.md)

## 本轮验证

当前源码候选：`d63d57d942f449fbd90d197e15e349eb63f4eefe`；tree `801e0c9ab9fc89d54d1fddbe31830d3fb213dd68`。

本轮重新验证，**不是整体全绿或可直接替换安装的结论**。旧 4.0.8 的 3,772 backend / 2,433 frontend / Windows 成绩不计入本次通过数字。

| 检查 | 本轮结果 |
| --- | --- |
| 完整 Linux 后端编译 | 通过，生成 17 个测试二进制 |
| Nextest CI profile | **3,797 passed / 9 skipped / 0 failed**，run `8a3bf116-c802-4cbb-ade3-d851aa3ce0dc`，239.192 秒；包含两条新增官方 orphan 组合回归 |
| Rust all-target Clippy，warnings denied | **未通过：E0463，应用 build-script 阶段无法找到 std**。同一 Rust 1.95.0 的完整构建与测试此前成功；原因尚未定位。只读诊断请求首次及授权后一次原样重试均被工具取消，已停止，未绕过。 |
| Rust 格式 / Git diff 检查 | 源码提交前通过；随后源码未变，仅补写本文档。 |
| 前端 frozen lock 安装 / TypeScript / Prettier / renderer build | 全部通过 |
| 前端完整 Vitest | **2,507 passed / 5 failed，206 文件中 204 通过、2 失败**，不是通过结果 |
| 前端失败文件无编译争用、单 worker、原 timeout 复测 | PiProviderForm **48/48**；App **19/20**。再单测 App basic provider flows 仍触发 **10 秒超时** |
| Windows-only / 当前 Windows Desktop GUI | 本轮未运行，不沿用旧候选成绩 |

前端的 787 个源码、测试、manifest/lock、构建配置文件与固定官方 main 逐字相同。最初与 Rust 编译并行时出现的 Pi 超时和后续重复 DOM 失败在隔离复测消失；App basic flow 超时仍可复现。已证明失败存在于“未改动的官方 main 前端 + 当前环境”组合，**尚未证明**是环境慢还是上游测试/应用缺陷。未提高 timeout、删除断言、改测试源码或宣称全绿。

- 新云端独立目录使用 Rust 1.95.0、Node 22.12.0、pnpm 10.12.3。
- 恢复官方 Debian 快照的隔离 GTK/WebKit/libsoup sysroot；不进行系统级安装。
- 不改既有安装、用户账号/真实配置/数据库或 Desktop 私有缓存。
- 本轮 Windows 新基线原生验收仍须另做；Linux 不能验证 Windows-only 分支，也不证明当前 Desktop GUI 的真实模型选择行为。
- 已获授权将源码与报告发布到新的独立候选分支 `codex/integration-main-20261011`，仅供复现和后续验收；不改旧分支、不改 PR #8120、不合并 upstream、不部署或覆盖安装。具体远端发布结果另以可核验提交为准。

## 下一步验收

这是待验收源码候选。剩余项为：在受支持的工具链上完成严格 Clippy；定位官方原样 App basic provider flow 的 10 秒测试超时；对新基线执行 Windows-only npm 修复/启动器和真实 Desktop 验收。既有 4.0.4 安装与归档保留，在这些限制解决或明确接受前不替换安装。
