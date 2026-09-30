# tenantsched — 单 CPU 租户调度模拟

纯 Go 模拟，不启动任何真实作业。

## 模型

- **组树**：最多 4 层、20 个节点，单根。每个组有每周期配额，每 `PeriodLen = 10`
  个整数 tick 在 tick 边界重置。
- **作业**：最多 40 个存活作业，携带释放时刻 `Release`、剩余工作量 `Work`、
  截止时刻 `Deadline`、所属组 `GroupID`。
- **每个 tick 的顺序**：
  1. 在 tick 边界应用此前已提交的变更（提交 / 迁组 / 取消）；
  2. 进入新周期时重置各级配额；
  3. 标记越过截止时刻仍未完成的作业为超期（产生超期证据，之后不再参与调度）；
  4. 从「已释放、未完成、未取消、未超期」的就绪作业中，按 **截止 tick 升序、
     作业 ID 升序** 选出最高优先级作业；
  5. 若该作业所属组链（根→叶）每一级都有配额，则执行 1 tick，并**同时**
     对链上每一级扣 1 配额；
  6. 否则该 tick 空转，记录阻挡祖先（最近的根方向零配额组）及全部被阻作业。
- **修订号（乐观并发控制）**：`Submit` / `Migrate` / `Cancel` / `AdvanceTo`
  都携带 `expectedRev`；不匹配返回 `ErrStaleRevision`，状态不变。每个已提交
  变更和每个已执行 tick 都是一个线性化点。
- **迁组不退款**：作业在旧链已消耗的配额保留；迁组从下一个 tick 边界起
  对新链计费。
- 所有状态迁移由一把 `sync.Mutex` 串行化，并发推进不可能重复执行同一 tick。

## 快速开始

```go
s, _ := tenantsched.New([]tenantsched.GroupSpec{
    {ID: "root", Quota: 3},
    {ID: "a", ParentID: "root", Quota: 2},
    {ID: "a1", ParentID: "a", Quota: 1},
})

rev, _ := s.Submit(s.Revision(), tenantsched.JobSpec{
    ID: "j1", Release: 0, Work: 5, Deadline: 20, GroupID: "a1",
})

res, _ := s.AdvanceTo(rev, 11) // 执行并包含 tick 11
fmt.Println(tenantsched.FormatTrace(s.Trace(), s.OverdueEvidence()))
```

`AdvanceTo(rev, t)` 执行到（含）tick `t`；再次推进到已执行的 tick 返回
`ErrNoAdvance`。

## 测试

- `refmodel_test.go`：**独立的逐 tick 参考实现**（外部测试包，不接触内部
  状态），自有组表 / 作业状态 / 周期重置 / 选择逻辑。
- `scenario_test.go`：同一脚本驱动真实实现与参考实现，逐 tick 比对选择、
  阻挡祖先、链配额、超期证据，并在每次推进后比对完整快照；另含周期边界、
  祖先耗尽、迁组不退款、选择/取消、超期证据、修订号冲突等定点场景。
- `differential_test.go`：固定种子随机差分（提交/迁组/取消/推进混合）。
- `concurrent_test.go`：并发屏障测试 —— 同一 tick 的 N 个推进恰有一个
  成功、tick 恰好执行一次；混合操作风暴下轨迹稠密、配额不越界。
- `example_test.go`：固定的可读轨迹示例。

```bash
go test ./...            # 常规
go test -race ./...      # 竞态检测
go test -run TestRandomDifferential -v ./...
```
