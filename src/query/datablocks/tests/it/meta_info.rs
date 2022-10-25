// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;

use common_datablocks::MetaInfo;
use common_exception::Result;

#[derive(Debug, PartialEq)]
struct TestMetaInfoA {
    field_a: usize,
    field_b: String,
}

impl MetaInfo for TestMetaInfoA {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn MetaInfo>) -> bool {
        match info.as_any().downcast_ref::<TestMetaInfoA>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

#[derive(Debug, PartialEq)]
struct TestPartInfoB {
    field_a: String,
    field_b: u64,
}

impl MetaInfo for TestPartInfoB {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn MetaInfo>) -> bool {
        match info.as_any().downcast_ref::<TestPartInfoB>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

#[test]
fn test_partial_equals_part_info() -> Result<()> {
    let info_a: Box<dyn MetaInfo> = Box::new(TestMetaInfoA {
        field_a: 123,
        field_b: String::from("456"),
    });

    let info_b: Box<dyn MetaInfo> = Box::new(TestPartInfoB {
        field_a: String::from("123"),
        field_b: 456,
    });

    assert_ne!(&info_a, &info_b);
    assert_eq!(&info_a, &info_a);
    assert_eq!(&info_b, &info_b);
    Ok(())
}
