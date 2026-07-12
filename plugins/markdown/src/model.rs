use lix_order_key::OrderKey;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NodeKind {
    Document,
    Paragraph,
    Heading,
    ThematicBreak,
    BlockQuote,
    List,
    ListItem,
    CodeBlock,
    HtmlBlock,
    Definition,
    FootnoteDefinition,
    Table,
    TableColumn,
    TableRow,
    TableCell,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NodeSnapshot {
    pub(crate) id: String,
    pub(crate) kind: NodeKind,
    pub(crate) parent_id: Option<String>,
    pub(crate) order_key: Option<String>,
    pub(crate) payload: Value,
    pub(crate) format: Value,
}

impl NodeSnapshot {
    pub(crate) fn parsed_order_key(&self) -> Result<Option<OrderKey>, String> {
        self.order_key
            .as_deref()
            .map(OrderKey::from_snapshot_string)
            .transpose()
    }

    pub(crate) fn content_signature(&self) -> String {
        let mut payload = self.payload.clone();
        remove_identity_fields(&mut payload);
        serde_json::to_string(&(self.kind, payload, &self.format))
            .expect("markdown node signature must serialize")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NodeTree {
    pub(crate) node: NodeSnapshot,
    pub(crate) children: Vec<Self>,
}

impl NodeTree {
    pub(crate) fn subtree_signature(&self) -> String {
        let children = self
            .children
            .iter()
            .map(Self::subtree_signature)
            .collect::<Vec<_>>();
        serde_json::to_string(&(self.node.content_signature(), children))
            .expect("markdown subtree signature must serialize")
    }

    pub(crate) fn visit_mut(&mut self, visitor: &mut impl FnMut(&mut NodeSnapshot)) {
        visitor(&mut self.node);
        for child in &mut self.children {
            child.visit_mut(visitor);
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct Projection {
    pub(crate) nodes_by_id: BTreeMap<String, NodeSnapshot>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct InlineNode {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,
    #[serde(flatten)]
    pub(crate) content: InlineContent,
}

impl InlineNode {
    pub(crate) fn kind_tag(&self) -> &'static str {
        self.content.kind_tag()
    }

    pub(crate) fn signature(&self) -> String {
        let mut value =
            serde_json::to_value(&self.content).expect("markdown inline signature must serialize");
        remove_identity_fields(&mut value);
        serde_json::to_string(&value).expect("markdown inline signature must serialize")
    }

    pub(crate) fn children(&self) -> Option<&[Self]> {
        self.content.children()
    }

    pub(crate) fn children_mut(&mut self) -> Option<&mut Vec<Self>> {
        self.content.children_mut()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "type", rename_all = "snake_case")]
pub(crate) enum InlineContent {
    Text {
        value: String,
    },
    Escape {
        value: char,
    },
    CharacterReference {
        value: String,
        format: CharacterReferenceFormat,
    },
    Emphasis {
        children: Vec<InlineNode>,
        format: DelimiterFormat,
    },
    Strong {
        children: Vec<InlineNode>,
        format: DelimiterFormat,
    },
    Delete {
        children: Vec<InlineNode>,
        format: DeleteFormat,
    },
    Code {
        value: String,
        format: InlineCodeFormat,
    },
    Link {
        destination: String,
        title: Option<String>,
        children: Vec<InlineNode>,
        format: ResourceFormat,
    },
    Image {
        destination: String,
        title: Option<String>,
        alt: Vec<InlineNode>,
        format: ResourceFormat,
    },
    LinkReference {
        identifier: String,
        children: Vec<InlineNode>,
        format: ReferenceFormat,
    },
    ImageReference {
        identifier: String,
        alt: Vec<InlineNode>,
        format: ReferenceFormat,
    },
    Autolink {
        destination: String,
        format: AutolinkFormat,
    },
    Html {
        value: String,
    },
    SoftBreak,
    LineBreak {
        format: LineBreakFormat,
    },
    FootnoteReference {
        identifier: String,
        format: FootnoteReferenceFormat,
    },
}

impl InlineContent {
    fn kind_tag(&self) -> &'static str {
        match self {
            Self::Text { .. } => "text",
            Self::Escape { .. } => "escape",
            Self::CharacterReference { .. } => "character_reference",
            Self::Emphasis { .. } => "emphasis",
            Self::Strong { .. } => "strong",
            Self::Delete { .. } => "delete",
            Self::Code { .. } => "code",
            Self::Link { .. } => "link",
            Self::Image { .. } => "image",
            Self::LinkReference { .. } => "link_reference",
            Self::ImageReference { .. } => "image_reference",
            Self::Autolink { .. } => "autolink",
            Self::Html { .. } => "html",
            Self::SoftBreak => "soft_break",
            Self::LineBreak { .. } => "line_break",
            Self::FootnoteReference { .. } => "footnote_reference",
        }
    }

    fn children(&self) -> Option<&[InlineNode]> {
        match self {
            Self::Emphasis { children, .. }
            | Self::Strong { children, .. }
            | Self::Delete { children, .. }
            | Self::Link { children, .. }
            | Self::LinkReference { children, .. } => Some(children),
            Self::Image { alt, .. } | Self::ImageReference { alt, .. } => Some(alt),
            _ => None,
        }
    }

    fn children_mut(&mut self) -> Option<&mut Vec<InlineNode>> {
        match self {
            Self::Emphasis { children, .. }
            | Self::Strong { children, .. }
            | Self::Delete { children, .. }
            | Self::Link { children, .. }
            | Self::LinkReference { children, .. } => Some(children),
            Self::Image { alt, .. } | Self::ImageReference { alt, .. } => Some(alt),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CharacterReferenceFormat {
    pub(crate) reference: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DelimiterFormat {
    pub(crate) marker: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeleteFormat {
    pub(crate) marker: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InlineCodeFormat {
    pub(crate) raw: String,
    pub(crate) fence_length: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResourceFormat {
    pub(crate) destination: String,
    pub(crate) title: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReferenceFormat {
    pub(crate) label: String,
    pub(crate) kind: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutolinkFormat {
    pub(crate) kind: String,
    pub(crate) original: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LineBreakFormat {
    pub(crate) kind: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FootnoteReferenceFormat {
    pub(crate) label: String,
}

pub(crate) fn inline_payload(inlines: Vec<InlineNode>) -> Value {
    serde_json::json!({ "inline": inlines })
}

pub(crate) fn parse_inline_payload(payload: &Value) -> Result<Vec<InlineNode>, String> {
    serde_json::from_value(
        payload
            .get("inline")
            .cloned()
            .ok_or_else(|| "text-bearing node payload must contain 'inline'".to_string())?,
    )
    .map_err(|error| format!("invalid inline payload: {error}"))
}

pub(crate) fn replace_column_ids(value: &mut Value, replacements: &BTreeMap<String, String>) {
    match value {
        Value::Object(object) => {
            if let Some(Value::String(column_id)) = object.get_mut("column_id")
                && let Some(replacement) = replacements.get(column_id)
            {
                *column_id = replacement.clone();
            }
            for child in object.values_mut() {
                replace_column_ids(child, replacements);
            }
        }
        Value::Array(array) => {
            for child in array {
                replace_column_ids(child, replacements);
            }
        }
        _ => {}
    }
}

pub(crate) fn semantic_payload(value: &Value) -> Value {
    let mut value = value.clone();
    remove_semantic_irrelevant_fields(&mut value);
    value
}

fn remove_semantic_irrelevant_fields(value: &mut Value) {
    match value {
        Value::Object(object) => {
            object.remove("id");
            object.remove("format");
            for child in object.values_mut() {
                remove_semantic_irrelevant_fields(child);
            }
        }
        Value::Array(array) => {
            for child in array {
                remove_semantic_irrelevant_fields(child);
            }
        }
        _ => {}
    }
}

fn remove_identity_fields(value: &mut Value) {
    match value {
        Value::Object(object) => {
            object.remove("id");
            object.remove("column_id");
            for child in object.values_mut() {
                remove_identity_fields(child);
            }
        }
        Value::Array(array) => {
            for child in array {
                remove_identity_fields(child);
            }
        }
        _ => {}
    }
}
