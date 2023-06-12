# rust learn

## 基础

* Rust内存数据布局
  * [布局-1](https://rustmagazine.github.io/rust_magazine_2021/chapter_6/ant-rust-data-layout.html)
  * [布局-2](https://github.com/rustlang-cn/Rustt/blob/main/Articles/%5B2022-05-04%5D%20%E5%8F%AF%E8%A7%86%E5%8C%96%20Rust%20%E5%90%84%E6%95%B0%E6%8D%AE%E7%B1%BB%E5%9E%8B%E7%9A%84%E5%86%85%E5%AD%98%E5%B8%83%E5%B1%80.md)
* `lifetime`
  * [生命周期](https://xie.infoq.cn/article/caae061fc5578083b1c45e470)
* [智能指针](https://course.rs/advance/smart-pointer/intro.html)
* Rust std容器使用
* 原子类
* 并发

## 三方库

* tokio
  * [使用 Tokio 处理 CPU 密集型任务](https://github.com/rustlang-cn/Rustt/blob/main/Articles/%5B2022-04-20%5D%20%E4%BD%BF%E7%94%A8%20Tokio%20%E5%A4%84%E7%90%86%20CPU%20%E5%AF%86%E9%9B%86%E5%9E%8B%E4%BB%BB%E5%8A%A1.md)
  * [tokio简介](https://rust-book.junmajinlong.com/ch100/00.html)
* bytes
* [async-rdma](https://rustmagazine.github.io/rust_magazine_2022/Q1/contribute/async-rdma.html)
* [tracing](https://github.com/rustlang-cn/Rustt/blob/main/Articles/%5B2022-04-07%5D%20%E5%9C%A8%20Rust%20%E4%B8%AD%E4%BD%BF%E7%94%A8%20tracing%20%E8%87%AA%E5%AE%9A%E4%B9%89%E6%97%A5%E5%BF%97.md)
* lazy_static
* [jni](https://rustcc.cn/article?id=4ca84a67-d972-4460-912e-a297ec5edc0a)
* [thiserror](https://crates.io/crates/thiserror)
* [nom](https://crates.io/crates/nom)
* [tempfile](https://crates.io/crates/tempfile)

## 项目规范

### git hook

> 拷贝或重新命名${project}/.git/hoos/pre-commit.sample

```shell
#!/bin/sh
#
# An example hook script to verify what is about to be committed.
# Called by "git commit" with no arguments.  The hook should
# exit with non-zero status after issuing an appropriate message if
# it wants to stop the commit.
#
# To enable this hook, rename this file to "pre-commit".

set -eu

if ! cargo fmt -- --check
then
    echo "There are some code style issues."
    echo "Run cargo fmt first."
    exit 1
fi

if ! cargo clippy --all-targets -- -D warnings
then
    echo "There are some clippy issues."
    exit 1
fi

if ! cargo test
then
    echo "There are some test issues."
    exit 1
fi

exit 0
```

> <https://deaddabe.fr/blog/2021/09/29/git-pre-commit-hook-for-rust-projects/>
> [三方库 rusty-hook](https://github.com/swellaby/rusty-hook)

### github ci

参考本项目 `.github/workflows`
