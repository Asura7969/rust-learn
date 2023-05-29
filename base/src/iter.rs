#[derive(Debug)]
pub struct MyIter(Vec<usize>);

impl Iterator for MyIter {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop()
    }
}

pub struct MyData(usize, usize);

impl Default for MyData {
    fn default() -> Self {
        MyData(0, 1)
    }
}

impl IntoIterator for MyData {
    type Item = usize;
    type IntoIter = MyIter;

    fn into_iter(self) -> Self::IntoIter {
        MyIter(vec![self.0, self.1])
    }
}

#[cfg(test)]
mod iter_tests {
    use crate::iter::MyData;

    #[test]
    #[ignore]
    pub fn test_into_iter() {
        let mut iter = MyData::default().into_iter();
        for value in iter.by_ref() {
            println!("value: {value:?}")
        }
        println!("{iter:?}")
    }
}
