use std::fmt::Error;

struct LoserTree {
    loser_tree: Vec<usize>,
    loser_tree_adjusted: bool,
    partitions: Vec<Option<Vec<i32>>>,
}

impl LoserTree {
    pub fn new(partitions: Vec<Option<Vec<i32>>>) -> Self {
        Self {
            loser_tree: vec![],
            loser_tree_adjusted: false,
            partitions,
        }
    }

    fn init_loser_tree(&mut self) {
        // Init loser tree
        self.loser_tree = vec![usize::MAX; self.partitions.len()];
        for i in 0..self.partitions.len() {
            let mut winner = i;
            let mut cmp_node = self.lt_leaf_node_index(i);
            while cmp_node != 0 && self.loser_tree[cmp_node] != usize::MAX {
                let challenger = self.loser_tree[cmp_node];
                if self.is_gt(winner, challenger) {
                    self.loser_tree[cmp_node] = winner;
                    winner = challenger;
                }

                cmp_node = self.lt_parent_node_index(cmp_node);
            }
            self.loser_tree[cmp_node] = winner;
        }
        self.loser_tree_adjusted = true;
    }

    #[inline]
    fn lt_parent_node_index(&self, node_idx: usize) -> usize {
        node_idx / 2
    }

    #[inline]
    fn lt_leaf_node_index(&self, cursor_index: usize) -> usize {
        (self.partitions.len() + cursor_index) / 2
    }

    fn update_loser_tree(&mut self) {
        let mut winner = self.loser_tree[0];
        // Replace overall winner by walking tree of losers
        let mut cmp_node = self.lt_leaf_node_index(winner);
        while cmp_node != 0 {
            let challenger = self.loser_tree[cmp_node];
            if self.is_gt(winner, challenger) {
                self.loser_tree[cmp_node] = winner;
                winner = challenger;
            }
            cmp_node = self.lt_parent_node_index(cmp_node);
        }
        self.loser_tree[0] = winner;
        self.loser_tree_adjusted = true;
    }

    fn next(&mut self) -> Result<Option<i32>, Error> {
        if self.loser_tree.is_empty() {
            self.init_loser_tree();
        }

        // Adjust the loser tree if necessary, returning control if needed
        if !self.loser_tree_adjusted {
            self.update_loser_tree();
        }

        let stream_idx = self.loser_tree[0];
        Ok(self.pop(stream_idx))
    }

    #[inline]
    fn is_gt(&self, a: usize, b: usize) -> bool {
        match (&self.partitions[a], &self.partitions[b]) {
            (None, _) => true,
            (_, None) => false,
            (Some(ac), Some(bc)) => match (ac.first(), bc.first()) {
                (None, _) => true,
                (_, None) => false,
                (Some(ai), Some(bi)) => {
                    return ai.cmp(bi).is_gt();
                },
            },
        }
    }

    fn pop(&mut self, stream_idx: usize) -> Option<i32> {
        let slot = &mut self.partitions[stream_idx];
        match slot {
            Some(s) => {
                self.loser_tree_adjusted = false;
                s.pop()
            },
            _ => None,
        }
    }
}

#[cfg(test)]
mod loser_tree_tests {
    use crate::loser_tree::LoserTree;

    #[test]
    pub fn test_loser_tree() {
        let mut loser_tree = LoserTree::new(vec![
            Some(vec![10, 9, 8]),
            Some(vec![7, 6, 5]),
            Some(vec![13, 12, 11]),
            Some(vec![3, 2, 1]),
        ]);

        fn print(v: i32) {
            println!("{v}");
        }
        print(loser_tree.next().unwrap().unwrap());
        print(loser_tree.next().unwrap().unwrap());
        print(loser_tree.next().unwrap().unwrap());
        print(loser_tree.next().unwrap().unwrap());
        print(loser_tree.next().unwrap().unwrap());
        print(loser_tree.next().unwrap().unwrap());
        print(loser_tree.next().unwrap().unwrap());
        print(loser_tree.next().unwrap().unwrap());
        print(loser_tree.next().unwrap().unwrap());
        print(loser_tree.next().unwrap().unwrap());
        print(loser_tree.next().unwrap().unwrap());
        print(loser_tree.next().unwrap().unwrap());
    }
}
