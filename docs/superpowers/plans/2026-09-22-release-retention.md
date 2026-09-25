# 受控发布物清理 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 发布成功后安全清理已识别旧版本，保留当前双服务、两个回滚版本和所有运行时依赖。

**Architecture:** 一个受控发布维护脚本完成inventory、preview和显式execute，不依赖Worker和数据库。构建阶段记录不可变文件清单；部署后验收登记发布时间/依赖/验证事实；删除前重新读取真实服务和进程引用。所有不确定对象默认保留。

**Tech Stack:** Python 3.12标准库pathlib/json/hashlib/subprocess/shutil、Linux systemd只读检查、pytest。

**Spec:** `docs/superpowers/specs/2026-09-22-storage-lifecycle-cleanup-design.md` §6、§8，ADR-0058（Accepted）；跟踪GitHub Issue #84。本计划独立于 `2026-09-22-storage-lifecycle-cleanup.md`。

## Global Constraints

- 发布清理作为部署成功后的显式阶段，默认预览。
- 保护集合包含Worker/API当前目标、运行中进程的代码位置、两个最近成功的不同回滚版本，以及这些版本递归引用的 `.venv`/共享运行时。
- 被引用的依赖所在旧发布目录整体保护；无法识别的旧目录、备份、含外置数据/配置的目录不自动删除。
- 执行前再次解析绝对路径并确认严格位于发布根内、不是根本身、没有变化或新增引用。
- Worker的 `ProtectHome`、`ReadWritePaths` 和数据库最小权限不为删除发布物而放宽。
- 不清理系统日志、Docker、系统缓存、共享运行时根、Raw、备份或其他项目。
- 不增加cron/systemd timer，系统层仍只保证Worker/API常驻。
- 使用标准库，无新依赖，无生产自动执行；此次用户批准设计不等于授权服务器删除。

## 文件结构与受控格式

| 文件 | 变更 |
| --- | --- |
| `scripts/build_release.py` | 在发布包中生成 `release-manifest.json`，不把本机凭据/Raw打包 |
| `scripts/cleanup_releases.py`（新） | CLI、清单核验、受保护集合、preview、双确认execute |
| `tests/test_build_release.py` | 包内清单、提交、文件hash与排除项 |
| `tests/test_cleanup_releases.py`（新） | tmp_path模拟版本、引用、未知目录和删除前竞态 |
| `docs/最小生产发布运行手册.md` | 部署验收登记、清理预览、确认、gzip回退兼容门禁 |

不改 `deploy.ps1`：它是本机部署脚本，不是生产旧目录管理。GitHub production.yml只做受保护schema/smoke，不扩展成远程shell删除入口。
新脚本可独立测试；import使用与test_build_release相同的spec_from_file_location模式，若定义dataclass，先将模块放入sys.modules再exec_module。

## Task 1: 可信发布清单与纯清理预览

**Interfaces:**
- `Release` dataclass：root:Path、commit:str、verified_at:datetime、dependencies:tuple[Path,...]、raw_reader_gzip:bool。
- `ReleasePlan` dataclass：protected:tuple[Path,...]、candidates:tuple[Path,...]、unknown:tuple[Path,...]、fingerprint:str。
- `plan_releases(releases: Sequence[Release], *, current: Sequence[Path], active_paths: Sequence[Path], require_gzip_reader: bool) -> ReleasePlan`；纯函数，不做文件删除/服务操作。
- CLI `python scripts/cleanup_releases.py preview` 默认等价无子命令；受控生产根和两个服务名固定常量，不接受任意glob目标。
- `register_verified_release(root: Path, *, verified_at: datetime, dependency_paths: tuple[Path, ...]) -> None`：只由同脚本 `register-verified --confirm` 在当前两个服务、migration和已有smoke只读检查均通过后调用；登记必须独占写入且复核manifest hash，不能手工传一个success布尔值跳过检查。

- [ ] **1. 写失败测试。** 在新测试用与test_build_release相同方式导入脚本，构造如下完整案例；路径是tmp_path，不是真的服务路径：

```python
from datetime import UTC, datetime, timedelta


def test_plan_protects_runtime_owner_and_two_rollbacks(tmp_path):
    now = datetime(2026, 9, 22, tzinfo=UTC)
    releases = tuple(
        cleanup.Release(
            tmp_path / f"r{i}",
            f"{i:040x}",
            now - timedelta(days=i),
            (tmp_path / "r4" / ".venv",) if i == 0 else (),
            True,
        )
        for i in range(5)
    )
    result = cleanup.plan_releases(
        releases, current=(tmp_path / "r0",), active_paths=(), require_gzip_reader=True
    )
    assert set(result.protected) == {tmp_path / f"r{i}" for i in (0, 1, 2, 4)}
    assert result.candidates == (tmp_path / "r3",)
```

- [ ] **2. 运行RED。** `uv run pytest tests/test_cleanup_releases.py tests/test_build_release.py -q`。
- [ ] **3. 构建清单。** 包内额外生成以下格式；files是包内所有代码/doc文件的相对路径+SHA256映射，不包括清单自身，不含工作站路径：

```json
{"schema_version": "release_manifest.v1", "commit": "40-character-git-sha",
 "version": "0.2.0", "raw_reader_gzip": true, "files": {"pyproject.toml": "sha256"}}
```

实际commit来自 `git rev-parse HEAD`，SHA来自包内最终字节（shell脚本LF标准化后再hash）。`raw_reader_gzip`只能在Raw Task1通过门禁并包含于同一commit时为true，不根据目录名猜测。`--allow-dirty`只可实验，dirty包不得登记verified release；标记dirty且默认不生成可清理候选。

生产验收登记 `release-verified.json`（运行时生成，不commit）字段：schema_version、commit、verified_at UTC、manifest_sha256、dependency_paths、验证状态。`register-verified --confirm`在部署维护锁内，针对固定两个服务当前根验证manifest文件hash、运行进程引用、systemd active、已有 `deploy/linux/smoke-check.sh`（含只读migration/Worker检查）及现有API只读健康门禁，任何一步失败不得登记。用subprocess参数列表和超时调用，错误只输出类型/受控代码，不回显包含环境或URL的stdout；单测mock所有外部命令。不能把“安装依赖成功”当API/Worker验收成功。已登记同commit和manifest的重复操作只核验，不更新首次verified_at；不同内容拒绝覆盖。旧目录无清单或无验证记录均unknown，不按mtime伪造历史。

- [ ] **4. 纯plan和只读inventory。** inventory读取固定 `/home/project` 下登记发布物，但不递归执行未知路径。真实引用来自两个固定服务的MainPID/ExecStart/WorkingDirectory、当前 `/home/project-worker` / `/home/project-api` symlink、进程exe/cwd/cmdline的路径型参数和 `.venv`解析结果；受保护脚本路径不可只保护 `/usr/bin/python`。遍历归属服务cgroup的子进程，无法枚举或权限不足时整个execute拒绝，不猜没有运行实例。不把cmdline原文写报告。

引用以“路径位于某release根内”映射至整个release，非等于根才保护。递归依赖用visited集合防环；外部依赖只保护不删除。当前Worker/API可以不同版本，回滚选最近两个不同commit且验收成功的**非当前**版本。gzip兼容要求由实际Raw压缩状态检查/受控部署验收记录提供，未知则保守只认可声明兼容的回滚版本；候选不足两个则停止自动清理，不把不兼容旧包称回滚保护。

清单完整匹配且仅含清单文件/登记/代码缓存/声明私有venv才可candidate；出现 `.env`、Raw、备份、外置配置、未知文件、未知symlink、未验证metadata，均protected/unknown。预览返回相对release名、原因、大小和fingerprint；不打印文件内容、凭据或数据库地址。fingerprint取排序后的release身份/清单SHA/引用关系/目录stat的规范JSON hash，不是简单mtime。
- [ ] **5. GREEN。** 测双服务不同版本、仅exe在系统python但脚本在旧release、递归runtime闭环、回滚不足、未知目录、dirty包、含.env、manifest hash不符、gzip不兼容、未知/挂掉进程、同根依赖和外部依赖。Run同RED命令。
- [ ] **6. 提交。** 精确暂存构建、新脚本和两个测试；`git commit -m "feat: inventory releases and protect rollback runtime references"`。

## Task 2: 显式执行与部署交付边界

**Interfaces:** `execute_plan(plan: ReleasePlan, *, release_root: Path, refreshed: ReleasePlan) -> tuple[Path, ...]`；仅执行fresh preview完全一致的candidate。CLI执行 `execute --confirm --preview-fingerprint HASH`，缺一报参数错误。固定生产root不暴露任意删除root参数，测试函数可接tmp_path。

- [ ] **1. 写失败测试。** 构造Task1得到的plan，用额外active_paths包含唯一candidate重新计算refreshed。断言 `execute_plan`抛 `RuntimeError` 且所有目录仍存在；默认CLI preview不能调用rmtree。另测release_root自身和root外路径被拒绝。
- [ ] **2. 运行RED。** `uv run pytest tests/test_cleanup_releases.py -q`。
- [ ] **3. 最小受控删除。** 只在Linux使用安全的同shell Python文件API，绝不拼shell rm命令。先确认 `shutil.rmtree.avoids_symlink_attacks` 为true，限制root由运维控制；重新读取service/cgroup/proc和文件manifest，fresh fingerprint必须一致。每个candidate删除前再次lstat及resolve：

```python
root = release_root.resolve(strict=True)
resolved = candidate.resolve(strict=True)
if candidate.is_symlink() or resolved == root or not resolved.is_relative_to(root):
    raise RuntimeError("unsafe release target")
if resolved in refreshed.protected or resolved not in refreshed.candidates:
    raise RuntimeError("release reference changed")
```

使用受控发布维护锁（Linux `fcntl.flock`，固定root内锁文件，只有execute/register获取），与未来发布验收登记同锁；文档说明所有切换版本操作必须持此维护锁。删除前服务状态变化即停，不能停止服务来让引用消失。递归删除用fd安全的rmtree，在最终身份检查后进行，不跟随外部symlink；无法证明唯一私有venv归属就保留整目录。默认preview不创建锁文件。

发布包 `.tar.gz/.sha256`只有与已识别candidate绑定、非最新保留版本且无其他引用时才允许列入同一显式预览；其余旧包留unknown，不泛化清理dist。删除后报告精确release名/文件字节/剩余free，不声称可恢复；原包或Git可重建代码，但未备份的依赖不保证恢复。
- [ ] **4. GREEN与只读验证。** 模拟预览到执行间新增runtime引用、删除前改symlink、root出界、私有venv出现外部引用、缺读proc权限、缺确认/不同fingerprint、第一次删成功第二次失败，报告必须准确且不继续误删。Windows测试仅pure-plan和安全拒绝分支，LinuxCI跑fd安全执行测试；不为了Windows测试跨shell删目录。

Run: `uv run pytest tests/test_cleanup_releases.py tests/test_build_release.py -q`；完整门禁复用主计划Task8。本计划单独交付也必须跑ruff/mypy/pytest，不能借用别的commit的结果。
- [ ] **5. 更新运行手册并提交。** 新增顺序“部署现有smoke通过→登记verified→只读preview→审核实际候选→当次授权execute”。明确现存旧依赖链保护、旧目录不能自动补假metadata、gzipreader回退限制、无备份不可恢复风险；不改生产workflow和Workerunit。`git commit -m "feat: add explicitly confirmed release cleanup after deployment"`（先精确暂存本任务文件）。

## Self-review与交付

- Spec§6发布物保护由Task1完整覆盖，路径/权限/删除前再核对由Task2覆盖。
- 不强依赖主计划数据库表；主计划可先交付。若尚未实现gzipreader，构建清单必须false，不能虚报能力。
- 此脚本不会首次部署就删除历史未知目录；若要回收旧存量，必须另行只读确认其身份、依赖和可恢复性，再获当次明确授权。
- 执行方式与主计划一致：当前目录的codex/分支内逐项实施，不创建worktree、不使用子代理。
  本配套计划尚未实施，不是已上线能力。
