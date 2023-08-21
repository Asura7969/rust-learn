# Rust Paimon

## Features

- [x] 读元数据（`snapshot`、`schema`、`manifest`、`manifestList`）
- [x] 支持读`manifest`数据`avro`格式
- [x] 支持读data数据`parquet`格式
- [x] 支持读`input`模式表数据
- [x] 支持批读

## Doing

- [ ] 集成`datafusion`

## RoadMap

- [ ] `manifest`数据支持读取`parquet`格式
- [ ] data数据格式支持`avro`格式
- [ ] 本地`paimon`表读取
- [ ] 集成`incubator-opendal`
- [ ] 支持`Appendonly`表读取数据
- [ ] 支持流读
- [ ] 支持`tag`功能
- [ ] 支持写功能
