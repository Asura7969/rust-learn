### 基本问题

#### 1、`Send` & `Sync`


#### 2、常用的智能指针
* **Box<T>**
* **Rc<T>**
* **Arc<T>**
* **Cell<T>**
* **RefCell<T>**


#### 3、常见的trait
* **Deref**: 是解引用语义，结合Rust中的自动解引用机制，可允许自定义包装类型如智能指针等变量像内部变量一样使用，但使用仅限于取`&self`，如果要取`&mut self`则需要`DerefMut`这个`trait`。
* **Drop**: 即析构函数。在变量生命周期结束时将自动执行`drop`以销毁。
* **Clone**: 是复制语义，可由用户实现，表示复制一个对象。任何可复制的类都可以实现这个`trait`。
* **Copy**: 也是复制语义，但与`Clone`不同的是，它只是一个标记，用来告诉编译器，该类型可以直接通过**memcpy**复制，而无需其他动作。`Copy`通常用于原生类型及其组合类型（结构体、元组、枚举等）。
* **Any**: 提供了一种简单的动态反射机制，要求类型必须具有`'static`的生命周期。在运行时可以`downcast`到任意类型，但若实际类型与要转换的类型不一致时，将返回`Err`。


#### 4、`DST`

#### 5、什么是`trait object`


#### 6、`&'static` 和 `T: 'static` 的用法有何区别？
* `'static`: 表示该引用的生命周期和程序活得一样长（但该引用的变量受作用域限制）
> [参考](https://course.rs/advance/lifetime/static.html)

#### 7、fn Fn FnMut FnOnce 的区别

#### 8、&str 、str 与 String


