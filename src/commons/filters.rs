use crate::models::model_filters::{LastModifyFilter, LastModifyFilterType};
use anyhow::{Context, Result};
use regex::RegexSet;

pub trait Filter<T> {
    fn intercepted(&self, content: T) -> bool;
}

#[derive(Debug, Clone)]
pub struct RegexFilter {
    pub exclude_regex: Option<RegexSet>,
    pub include_regex: Option<RegexSet>,
}

impl Default for RegexFilter {
    fn default() -> Self {
        Self {
            exclude_regex: None,
            include_regex: None,
        }
    }
}

impl RegexFilter {
    pub fn from_vec(
        exclude_regex: &Option<Vec<String>>,
        include_regex: &Option<Vec<String>>,
    ) -> Result<Self> {
        let exclude_regex = match exclude_regex {
            Some(v) => Some(RegexSet::new(v)?),
            None => None,
        };
        let include_regex = match include_regex {
            Some(v) => Some(RegexSet::new(v)?),
            None => None,
        };

        Ok(Self {
            exclude_regex,
            include_regex,
        })
    }

    pub fn from_vec_option(
        exclude_regex: &Option<Vec<String>>,
        include_regex: &Option<Vec<String>>,
    ) -> Result<Option<Self>> {
        let exclude_regex = match exclude_regex {
            Some(v) => Some(RegexSet::new(v).context(format!("{}:{}", file!(), line!()))?),
            None => None,
        };
        let include_regex = match include_regex {
            Some(v) => Some(RegexSet::new(v).context(format!("{}:{}", file!(), line!()))?),
            None => None,
        };

        if exclude_regex.is_none() && include_regex.is_none() {
            return Ok(None);
        }

        Ok(Some(Self {
            exclude_regex,
            include_regex,
        }))
    }

    #[allow(dead_code)]
    pub fn new(exclude_regex: Option<RegexSet>, include_regex: Option<RegexSet>) -> Self {
        Self {
            exclude_regex,
            include_regex,
        }
    }

    pub fn excluded(&self, content: &str) -> bool {
        let is_exclude = match self.exclude_regex.clone() {
            Some(e) => {
                return e.is_match(content);
            }
            None => false,
        };
        is_exclude
    }

    pub fn included(&self, content: &str) -> bool {
        let is_included = match self.include_regex.clone() {
            Some(e) => {
                return e.is_match(content);
            }
            None => true,
        };
        is_included
    }

    /// 过滤逻辑
    // 通过筛选器，未被规则拦截返回 true
    pub fn passed(&self, content: &str) -> bool {
        if self.excluded(content) {
            return false;
        }

        if !self.included(content) {
            return false;
        }

        true
    }

    // 内容被过滤器拦截，符合拦截条件返回 true
    pub fn intercepted(&self, content: &str) -> bool {
        if self.excluded(content) {
            return true;
        }

        if !self.included(content) {
            return true;
        }

        false
    }
}

impl Filter<&str> for RegexFilter {
    fn intercepted(&self, content: &str) -> bool {
        if self.excluded(content) {
            return true;
        }

        if !self.included(content) {
            return true;
        }

        false
    }
}

impl LastModifyFilter {
    // 通过筛选器，未被规则拦截返回 true
    pub fn passed(&self, timestamp: usize) -> bool {
        match self.filter_type {
            LastModifyFilterType::Greater => timestamp.ge(&self.timestamp),
            LastModifyFilterType::Less => timestamp.le(&self.timestamp),
        }
    }

    pub fn intercepted(&self, timestamp: usize) -> bool {
        match self.filter_type {
            LastModifyFilterType::Greater => timestamp.lt(&self.timestamp),
            LastModifyFilterType::Less => timestamp.gt(&self.timestamp),
        }
    }
}
