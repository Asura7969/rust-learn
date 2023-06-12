# 基本问题

## 1、`Send` & `Sync`

`Send`：可以在线程间安全的传递其所有权
`Sync`：可以在线程间安全的共享(通过引用)

* 裸指针两者都没实现，因为它本身就没有任何安全保证

* `UnsafeCell`不是`Sync`，因此`Cell`和`RefCell`也不是
* `Rc`两者都没实现(因为内部的引用计数器不是线程安全的)

## 2、常用的智能指针

* **Box<T>**: 可以将值分配到堆上

```rust
  let x: Box<i32> = Box::new(5);
  let ptr: *mut i32 = Box::into_raw(x);
  let x: Box<i32>  = unsafe { Box::from_raw(ptr) };
```

* **Rc<T>**: 引用计数类型，允许多所有权存在，只能用于同一线程内部（只读引用，不能修改内部值）
* **Arc<T>**：原子化实现的引用计数，允许多所有权存在，可以在多线程内部使用（只读引用，不能修改内部值）
* **Cell<T>**：适用于`T`实现`Copy`的情况

```rust
  use std::cell::Cell;
  fn main() {
      let c = Cell::new("asdf");
      let one = c.get();
      c.set("qwer");
      let two = c.get();
      println!("{},{}", one, two);
  }
```

* **RefCell<T>**: 用于提供引用

## 3、常见的trait

* **Deref**: 是解引用语义，结合Rust中的自动解引用机制，可允许自定义包装类型如智能指针等变量像内部变量一样使用，但使用仅限于取`&self`，如果要取`&mut self`则需要`DerefMut`这个`trait`。
* **Drop**: 即析构函数。在变量生命周期结束时将自动执行`drop`以销毁。
* **Clone**: 是复制语义，可由用户实现，表示复制一个对象。任何可复制的类都可以实现这个`trait`。
* **Copy**: 也是复制语义，但与`Clone`不同的是，它只是一个标记，用来告诉编译器，该类型可以直接通过**memcpy**复制，而无需其他动作。`Copy`通常用于原生类型及其组合类型（结构体、元组、枚举等）。
* **Any**: 提供了一种简单的动态反射机制，要求类型必须具有`'static`的生命周期。在运行时可以`downcast`到任意类型，但若实际类型与要转换的类型不一致时，将返回`Err`。

## 4、`DST`

动态大小类型(dynamically sized type), 编译时无法确定其大小,包括 str,[T]以及`Trait object`

## 5、什么是`trait object`

大小不固定，通常使用引用的方式，引用类型大小固定，它由两个指针组成，因此占用两个指针大小，即两个机器字长

```rust
                     address
                   /   vtable
                 /   /
               /   /
            +–––+–––+
stack frame │ • │ • │
            +–│–+–––+
              │      \
              │       \
              │        \
            +–V–+––––+ +–-–+–––+
       heap │Instance│ │ table │
            +–––+––––+ +–––+–––+
             
```

> [trait object](https://rust-book.junmajinlong.com/ch11/04_trait_object.html)

## 6、`&'static` 和 `T: 'static` 的用法有何区别？

* `&'static`: 表示该引用的生命周期和程序活得一样长（但该引用的变量受作用域限制）

```rust
fn print_it<T: Debug + 'static>(input: T)

fn print_it1(input: impl Debug + 'static)

#[derive(Debug)]
struct MyData;

static STATIC_MYDATA: MyData = MyData;
const CONST_MYDATA: MyData = MyData;

fn main() {
    let i = 5;
    let s:&'static str = "s";
    // 传入值类型结果
    // i: done
    // &i: error
    // s: done
    // CONST_MYDATA: done
    // &CONST_MYDATA: done
    // &STATIC_MYDATA: done
}
```

如上两个方法, 如果入参是所有权变量，则正常通过;
如果入参是引用变量, 若引用的生命周期不是`'static`, 或该引用指向的值不是 **static** 和 **常量** 会报错, 反之则不会

* `T: 'static` 应该读作`T`以`'static`生命周期为界
* 如果`T: 'static`，则`T`可以是具有`'static`的借用类型或所有权类型
* 由于`T: 'static`包括拥有的类型，这意味着`T`
  * 可以在运行时动态分配
  * 不一定对整个程序有效
  * 可以安全自由地变异（修改）
  * 可以在运行时动态丢弃
  * 可以有不同持续时间的生命周期

> * <https://course.rs/advance/lifetime/static.html>
> * <https://doc.rust-lang.org/rust-by-example/scope/lifetime/static_lifetime.html>
> * <https://juejin.cn/post/7197043415144972346>

## 7、fn Fn FnMut FnOnce 的区别

* `Fn`: 是闭包的基本 Trait, 即闭包中仅有 reference ( &self)
* `FnMut`: FnMut: 旨在强调传入的闭包中含有可 reference ( &mut self)
* `FnOnce`: 旨在强调传入的闭包中含有 moved 进来的变量 ( self), 即意味着这个闭包只能被调用一次.

## 8、&str、str 与 String

```rust
let mut my_name = "Pascal".to_string();
my_name.push_str( " Precht");

let last_name = &my_name[7..];
```

* String: 字符串, 指针（内存地址、长度、容量）在栈上, 数据在堆上
* &str: 字符串切片引用, 指针在栈上（内存地址、长度）, 数据在编译后的二进制中, 具有static生命周期
* str: 字符串切片, 栈上的指针不存容量,只存长度,容量由实际字符串管理,动态大小类型

```rust
                     buffer
                   /   capacity
                 /   /  length
               /   /   /
            +–––+–––+–––+
stack frame │ • │ 8 │ 6 │ <- my_name: String
            +–│–+–––+–––+
              │
            [–│–––––––– capacity –––––––––––]
              │
            +–V–+–––+–––+–––+–––+–––+–––+–––+
       heap │ P │ a │ s │ c │ a │ l │   │   │
            +–––+–––+–––+–––+–––+–––+–––+–––+
            [––––––– length ––––––––]



            my_name: String   last_name: &str
            [––––––––––––]    [–––––––]
            +–––+––––+––––+–––+–––+–––+
stack frame │ • │ 16 │ 13 │   │ • │ 6 │
            +–│–+––––+––––+–––+–│–+–––+
              │                 │
              │                 +–––––––––+
              │                           │
              │                           │
              │                         [–│––––––– str –––––––––]
            +–V–+–––+–––+–––+–––+–––+–––+–V–+–––+–––+–––+–––+–––+–––+–––+–––+
       heap │ P │ a │ s │ c │ a │ l │   │ P │ r │ e │ c │ h │ t │   │   │   │
            +–––+–––+–––+–––+–––+–––+–––+–––+–––+–––+–––+–––+–––+–––+–––+–––+
```

> [翻译 Rust中的String和&str](https://zhuanlan.zhihu.com/p/123278299)

## 9、T 、&T 与 &mut T

| Type Variable   | T                                                   | &T                          | &mut T                                  |
|-----------------|-----------------------------------------------------|-----------------------------|-----------------------------------------|
| Examples        | i32, &i32, &mut i32, <br/>&&i32, &mut &mut i32, ... | &i32, &&i32, &&mut i32, ... | &mut i32, &mut &mut i32, &mut &i32, ... |

* `T`是`&T`和`&mut T`的超集
* `&T`和`&mut T`是不相交集

> [Common Rust Lifetime Misconceptions](https://juejin.cn/post/7197043415144972346#heading-4)

## 10、&'a T 与 T: 'a

* `T: 'a`比`&'a`更通用、更灵活
* `T: 'a`接受所有权类型，包含引用的所有权类型和引用
* `&'a T` 只接受引用
* 如果`T: 'static`, 由于`'static` >= `'a`，所以`'a`是所有

> [&'a T 与 T: 'a](https://juejin.cn/post/7197043415144972346#heading-6)

## 11、[Rust异步](https://xie.infoq.cn/article/8ca67f4929c89e0013f83ebc3)

## 12、[Rust 的 Pin 与 Unpin](https://folyd.com/blog/rust-pin-unpin/)

## 13、Object safety

> 为了解决在代码编写中，一些原本只能使用trait泛型的地方地方能够使用trait对象来替代，详情见[RFC-255](https://github.com/rust-lang/rfcs/blob/master/text/0255-object-safety.md)

满足`Object Safe`的条件:

* 所有方法都是`Object Safe`的
* Trait本身没有`Self: Sized`的约束

什么方法是`Object Safe`?

* 函数有Self: Sized约束时，或者或者符合第2点要求
  *（1）函数不能有泛型参数
  *（2）返回值不能使用Self类型

```rust
fn main() {
    let foo = Foo;
    static_dispatch(&foo);
    dynamic_dispatch(&foo);
}

#[derive(Debug)]
struct Foo;

trait Bar {
    fn baz(&self);
}

impl Bar for Foo {
    fn baz(&self) {
        println!("{:?}", self);
    }
}

// 静态分发
fn static_dispatch<T>(t: &T) where T: Bar {
    t.baz();
}

// 动态分发
fn dynamic_dispatch(t: &dyn Bar) {
    t.baz();
}
```

`静态分发`，编译器会使代码膨胀
`动态分发`，会有运行时开销，运行时需要从虚表中寻找对应函数
