//! Type descriptors: the type of a task's output, as recorded in DAG JSON.
//!
//! The Python runner derives them from the return annotation at parse time (see PROJECT.md,
//! Output serialization). The master uses them to check output references and paths without
//! running Python.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

/// The type of a value that can be stored as a task output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TypeDesc {
    /// Anything (an explicit `Any`): only the run-time check applies.
    Any,
    /// `None`.
    None,
    /// `bool`.
    Bool,
    /// `int`.
    Int,
    /// `float`.
    Float,
    /// `str`.
    Str,
    /// `datetime` (stored tagged).
    Datetime,
    /// `date` (stored tagged).
    Date,
    /// `timedelta` (stored tagged).
    Timedelta,
    /// `list[T]`.
    List {
        /// The element type.
        item: Box<TypeDesc>,
    },
    /// `tuple[A, B, ...]` (stored tagged).
    Tuple {
        /// The element types, in order.
        items: Vec<TypeDesc>,
    },
    /// `dict[str, T]` (keys are always strings).
    Dict {
        /// The value type.
        value: Box<TypeDesc>,
    },
    /// `Literal[...]`.
    Literal {
        /// The allowed values (strings, integers, booleans or `null`).
        values: Vec<serde_json::Value>,
    },
    /// A union, including `Optional[T]` (`T | None`).
    Union {
        /// The member types.
        variants: Vec<TypeDesc>,
    },
    /// A `TypedDict`.
    TypedDict {
        /// The class name, for messages.
        name: String,
        /// The keys, in declaration order.
        fields: Vec<Field>,
    },
    /// A dataclass.
    Dataclass {
        /// The class name, for messages and outcome names.
        name: String,
        /// The fields, in declaration order.
        fields: Vec<Field>,
    },
    /// The output of a `command` or `script` task.
    ProcessResult,
}

/// A named field of a `TypedDict` or dataclass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Field {
    /// The field or key name.
    pub name: String,
    /// The field's type.
    #[serde(rename = "type")]
    pub ty: TypeDesc,
    /// Whether the field must be present (`TypedDict`) or has no default (dataclass).
    pub required: bool,
}

impl TypeDesc {
    /// The type of the named field or key, for checking an output path such as `out.rows`.
    ///
    /// Returns `None` if this type has no such field: `TypedDict`s, dataclasses and
    /// `ProcessResult` have named fields, `dict[str, T]` has any key (giving `T`), and `Any`
    /// gives `Any`.
    #[must_use]
    pub fn field(&self, name: &str) -> Option<&TypeDesc> {
        static STR: TypeDesc = TypeDesc::Str;
        static INT: TypeDesc = TypeDesc::Int;
        static BOOL: TypeDesc = TypeDesc::Bool;
        match self {
            Self::Any => Some(self),
            Self::Dict { value } => Some(value),
            Self::ProcessResult => match name {
                "stdout" | "stderr" => Some(&STR),
                "exit_code" => Some(&INT),
                "stdout_truncated" | "stderr_truncated" => Some(&BOOL),
                _ => None,
            },
            Self::TypedDict { fields, .. } | Self::Dataclass { fields, .. } => {
                fields.iter().find(|f| f.name == name).map(|f| &f.ty)
            }
            _ => None,
        }
    }

    /// The type of the element at `index`, for checking an output path such as `out[0]`.
    ///
    /// Returns `None` if this type can't be indexed, or for a tuple index out of range;
    /// `Any` gives `Any`.
    #[must_use]
    pub fn element(&self, index: u64) -> Option<&TypeDesc> {
        match self {
            Self::Any => Some(self),
            Self::List { item } => Some(item),
            Self::Tuple { items } => usize::try_from(index).ok().and_then(|i| items.get(i)),
            _ => None,
        }
    }
}
