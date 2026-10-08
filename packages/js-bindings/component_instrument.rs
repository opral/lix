//! Locally trusted instrumentation for the core modules emitted by jco.
//!
//! This deliberately uses wasmparser/wasm-encoder's instruction-level
//! reencoder instead of rebuilding an AST. Function indices are shifted by the
//! one appended deadline import through `Reencode::function_index`, which
//! covers code, constant expressions, element segments, exports, and start.

use wasm_encoder::reencode::{self, Reencode};
use wasm_encoder::{
    CodeSection, EntityType, ExportKind, ExportSection, ImportSection, Instruction,
    MemoryType as EncodedMemoryType, Module, SectionId, TableType as EncodedTableType, TypeSection,
};
use wasmparser::{Parser, Payload, TypeRef, Validator, WasmFeatures};

const TICK_MODULE: &str = "lix:runtime/deadline";
const TICK_NAME: &str = "tick";
const RUNTIME_MEMORY_EXPORT: &str = "__lix_runtime_memory";
const MAX_CORE_INPUT_BYTES: usize = 64 * 1024 * 1024;
const MAX_CORE_OUTPUT_BYTES: u64 = 128 * 1024 * 1024;
const MAX_TABLE_ELEMENTS: u64 = 1_000_000;
const WASM_PAGE_BYTES: u64 = 65_536;
const WASM32_MAX_PAGES: u64 = 65_536;

#[derive(Debug)]
struct ModuleMetadata {
    imported_functions: u32,
    type_count: u32,
    memory_count: u32,
}

fn checked_add(left: u32, right: u32, what: &str) -> Result<u32, String> {
    left.checked_add(right)
        .ok_or_else(|| format!("{what} exceeds the WebAssembly index limit"))
}

fn var_u32_len(mut value: u32) -> u64 {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn validator_features() -> WasmFeatures {
    let mut features = WasmFeatures::default();
    // Keep the browser host's current supported core ISA and resource model.
    features.set(WasmFeatures::SIMD, false);
    features.set(WasmFeatures::RELAXED_SIMD, false);
    features.set(WasmFeatures::THREADS, false);
    features.set(WasmFeatures::MEMORY64, false);
    features.set(WasmFeatures::MULTI_MEMORY, false);
    features.set(WasmFeatures::CUSTOM_PAGE_SIZES, false);
    features
}

fn inspect_module(bytes: &[u8], maximum_pages: u64) -> Result<ModuleMetadata, String> {
    let mut metadata = ModuleMetadata {
        imported_functions: 0,
        type_count: 0,
        memory_count: 0,
    };

    for payload in Parser::new(0).parse_all(bytes) {
        match payload.map_err(|error| format!("Invalid core WebAssembly module: {error}"))? {
            Payload::TypeSection(section) => {
                for group in section {
                    let group = group.map_err(|error| error.to_string())?;
                    let count = u32::try_from(group.types().len())
                        .map_err(|_| "Too many WebAssembly types".to_owned())?;
                    metadata.type_count = checked_add(metadata.type_count, count, "Type count")?;
                }
            }
            Payload::ImportSection(section) => {
                for import in section.into_imports() {
                    let import = import.map_err(|error| error.to_string())?;
                    if import.module == TICK_MODULE {
                        return Err(format!("Reserved runtime import namespace '{TICK_MODULE}'"));
                    }
                    match import.ty {
                        TypeRef::Func(_) | TypeRef::FuncExact(_) => {
                            metadata.imported_functions = checked_add(
                                metadata.imported_functions,
                                1,
                                "Function import count",
                            )?;
                        }
                        TypeRef::Memory(_) => {
                            return Err("Imported component memories are unsupported".to_owned());
                        }
                        TypeRef::Table(table) => check_table_type(table)?,
                        _ => {}
                    }
                }
            }
            Payload::MemorySection(section) => {
                for memory in section {
                    let memory = memory.map_err(|error| error.to_string())?;
                    metadata.memory_count = checked_add(metadata.memory_count, 1, "Memory count")?;
                    check_memory_type(memory, maximum_pages)?;
                    if metadata.memory_count > 1 {
                        return Err("Multiple component memories are unsupported".to_owned());
                    }
                }
            }
            Payload::TableSection(section) => {
                for table in section {
                    let table = table.map_err(|error| error.to_string())?;
                    check_table_type(table.ty)?;
                }
            }
            Payload::ExportSection(section) => {
                for export in section {
                    let export = export.map_err(|error| error.to_string())?;
                    if export.name == RUNTIME_MEMORY_EXPORT {
                        return Err("Reserved runtime memory export".to_owned());
                    }
                }
            }
            _ => {}
        }
    }

    Ok(metadata)
}

fn check_memory_type(memory: wasmparser::MemoryType, maximum_pages: u64) -> Result<(), String> {
    if memory.shared || memory.memory64 {
        return Err("Shared and 64-bit component memories are unsupported".to_owned());
    }
    if memory.page_size_log2.is_some() {
        return Err("Custom-page-size component memories are unsupported".to_owned());
    }
    if memory.initial > maximum_pages {
        return Err("Component initial memory exceeds limit".to_owned());
    }
    Ok(())
}

fn check_table_type(table: wasmparser::TableType) -> Result<(), String> {
    if table.table64 || table.shared {
        return Err("64-bit or shared component tables are unsupported".to_owned());
    }
    if table.initial > MAX_TABLE_ELEMENTS {
        return Err("Component table exceeds element limit".to_owned());
    }
    Ok(())
}

struct Instrumenter {
    imported_functions: u32,
    tick_function_index: u32,
    tick_type_index: u32,
    memory_count: u32,
    maximum_pages: u64,
    output_estimate: u64,
    runtime_type_added: bool,
    runtime_import_added: bool,
    runtime_export_added: bool,
    export_section_seen: bool,
}

impl Instrumenter {
    fn new(metadata: ModuleMetadata, maximum_pages: u64, input_len: usize) -> Result<Self, String> {
        let tick_type_index = metadata.type_count;
        let tick_function_index = metadata.imported_functions;
        let output_estimate = u64::try_from(input_len)
            .map_err(|_| "Core module is too large".to_owned())?
            .checked_add(256)
            .ok_or_else(|| "Instrumented core module is too large".to_owned())?;
        if output_estimate > MAX_CORE_OUTPUT_BYTES {
            return Err("Instrumented core module exceeds output size limit".to_owned());
        }
        Ok(Self {
            imported_functions: metadata.imported_functions,
            tick_function_index,
            tick_type_index,
            memory_count: metadata.memory_count,
            maximum_pages,
            output_estimate,
            runtime_type_added: false,
            runtime_import_added: false,
            runtime_export_added: false,
            export_section_seen: false,
        })
    }

    fn reserve_output(&mut self, amount: u64) -> Result<(), reencode::Error<String>> {
        let next = self.output_estimate.checked_add(amount).ok_or_else(|| {
            reencode::Error::UserError("Instrumented core module is too large".into())
        })?;
        if next > MAX_CORE_OUTPUT_BYTES {
            return Err(reencode::Error::UserError(
                "Instrumented core module exceeds output size limit".into(),
            ));
        }
        self.output_estimate = next;
        Ok(())
    }

    fn runtime_type(&mut self, module: &mut Module) {
        let mut types = TypeSection::new();
        types.ty().function([], []);
        module.section(&types);
        self.runtime_type_added = true;
    }

    fn runtime_import(&mut self, module: &mut Module) {
        let mut imports = ImportSection::new();
        imports.import(
            TICK_MODULE,
            TICK_NAME,
            EntityType::Function(self.tick_type_index),
        );
        module.section(&imports);
        self.runtime_import_added = true;
    }

    fn runtime_memory_export(&mut self, module: &mut Module) {
        let mut exports = ExportSection::new();
        exports.export(RUNTIME_MEMORY_EXPORT, ExportKind::Memory, 0);
        module.section(&exports);
        self.runtime_export_added = true;
    }

    fn add_tick_import(
        &mut self,
        imports: &mut ImportSection,
    ) -> Result<(), reencode::Error<String>> {
        // Existing imports stay in their original order. Appending this function
        // import leaves every imported function index unchanged and shifts only
        // the defined-function range by one.
        imports.import(
            TICK_MODULE,
            TICK_NAME,
            EntityType::Function(self.tick_type_index),
        );
        self.runtime_import_added = true;
        Ok(())
    }

    fn capped_memory_type(
        &mut self,
        memory: wasmparser::MemoryType,
    ) -> Result<EncodedMemoryType, reencode::Error<String>> {
        if let Err(error) = check_memory_type(memory, self.maximum_pages) {
            return Err(reencode::Error::UserError(error));
        }
        let maximum = memory
            .maximum
            .map(|existing| existing.min(self.maximum_pages))
            .unwrap_or(self.maximum_pages.min(WASM32_MAX_PAGES));
        if memory.maximum.is_none() {
            let maximum = u32::try_from(maximum).map_err(|_| {
                reencode::Error::UserError("Memory page limit is out of range".into())
            })?;
            self.reserve_output(var_u32_len(maximum))?;
        }
        Ok(EncodedMemoryType {
            minimum: memory.initial,
            maximum: Some(maximum),
            memory64: false,
            shared: false,
            page_size_log2: None,
        })
    }

    fn capped_table_type(
        &mut self,
        table: wasmparser::TableType,
    ) -> Result<EncodedTableType, reencode::Error<String>> {
        if let Err(error) = check_table_type(table) {
            return Err(reencode::Error::UserError(error));
        }
        let maximum = table
            .maximum
            .map(|existing| existing.min(MAX_TABLE_ELEMENTS))
            .unwrap_or(MAX_TABLE_ELEMENTS);
        if table.maximum.is_none() {
            let maximum = u32::try_from(maximum).map_err(|_| {
                reencode::Error::UserError("Table element limit is out of range".into())
            })?;
            self.reserve_output(var_u32_len(maximum))?;
        }
        Ok(EncodedTableType {
            element_type: self.ref_type(table.element_type)?,
            minimum: table.initial,
            maximum: Some(maximum),
            table64: false,
            shared: false,
        })
    }
}

impl Reencode for Instrumenter {
    type Error = String;

    fn function_index(&mut self, function: u32) -> Result<u32, reencode::Error<Self::Error>> {
        if function < self.imported_functions {
            return Ok(function);
        }
        let shifted = function
            .checked_add(1)
            .ok_or_else(|| reencode::Error::UserError("Function index overflow".into()))?;
        let growth = var_u32_len(shifted).saturating_sub(var_u32_len(function));
        self.reserve_output(growth)?;
        Ok(shifted)
    }

    fn memory_type(
        &mut self,
        memory: wasmparser::MemoryType,
    ) -> Result<EncodedMemoryType, reencode::Error<Self::Error>> {
        self.capped_memory_type(memory)
    }

    fn table_type(
        &mut self,
        table: wasmparser::TableType,
    ) -> Result<EncodedTableType, reencode::Error<Self::Error>> {
        self.capped_table_type(table)
    }

    fn parse_type_section(
        &mut self,
        types: &mut TypeSection,
        section: wasmparser::TypeSectionReader<'_>,
    ) -> Result<(), reencode::Error<Self::Error>> {
        reencode::utils::parse_type_section(self, types, section)?;
        if self.tick_type_index == u32::MAX {
            return Err(reencode::Error::UserError("Type index overflow".into()));
        }
        types.ty().function([], []);
        self.runtime_type_added = true;
        Ok(())
    }

    fn parse_import_section(
        &mut self,
        imports: &mut ImportSection,
        section: wasmparser::ImportSectionReader<'_>,
    ) -> Result<(), reencode::Error<Self::Error>> {
        reencode::utils::parse_import_section(self, imports, section)?;
        self.add_tick_import(imports)
    }

    fn parse_export_section(
        &mut self,
        exports: &mut ExportSection,
        section: wasmparser::ExportSectionReader<'_>,
    ) -> Result<(), reencode::Error<Self::Error>> {
        reencode::utils::parse_export_section(self, exports, section)?;
        self.export_section_seen = true;
        if self.memory_count == 1 {
            exports.export(RUNTIME_MEMORY_EXPORT, ExportKind::Memory, 0);
            self.runtime_export_added = true;
        }
        Ok(())
    }

    fn parse_custom_section(
        &mut self,
        _module: &mut Module,
        _section: wasmparser::CustomSectionReader<'_>,
    ) -> Result<(), reencode::Error<Self::Error>> {
        // Index-bearing and linker-specific custom sections are intentionally
        // stripped. The executable standard sections and data remain intact.
        Ok(())
    }

    fn parse_function_body(
        &mut self,
        code: &mut CodeSection,
        body: wasmparser::FunctionBody<'_>,
    ) -> Result<(), reencode::Error<Self::Error>> {
        let tick_instruction_bytes = 1 + var_u32_len(self.tick_function_index);
        // One tick is emitted at function entry. The body-size LEB can grow by
        // at most five bytes, the full width of a valid u32.
        self.reserve_output(tick_instruction_bytes + 5)?;
        let mut function = self.new_function_with_parsed_locals(&body)?;
        function.instruction(&Instruction::Call(self.tick_function_index));

        let mut operators = body.get_operators_reader()?;
        while !operators.eof() {
            let operator = operators.read()?;
            let is_loop = matches!(operator, wasmparser::Operator::Loop { .. });
            let instruction = self.instruction(operator)?;
            function.instruction(&instruction);
            if is_loop {
                self.reserve_output(tick_instruction_bytes)?;
                function.instruction(&Instruction::Call(self.tick_function_index));
            }
        }
        code.function(&function);
        Ok(())
    }

    fn intersperse_section_hook(
        &mut self,
        module: &mut Module,
        _after: Option<SectionId>,
        before: Option<SectionId>,
    ) -> Result<(), reencode::Error<Self::Error>> {
        let next = before.map(u8::from).unwrap_or(u8::MAX);
        if before != Some(SectionId::Type) && !self.runtime_type_added {
            if self.tick_type_index == u32::MAX {
                return Err(reencode::Error::UserError("Type index overflow".into()));
            }
            self.runtime_type(module);
        }

        // If an original import section is next, parse_import_section appends
        // the runtime import to that section. Otherwise create it at the first
        // legal boundary after the import section.
        if next > u8::from(SectionId::Import) && !self.runtime_import_added {
            self.runtime_import(module);
        }

        // Exports must precede start/element/code/data sections. If the input
        // had no export section, synthesize one at that boundary for memory
        // high-water accounting.
        if self.memory_count == 1
            && !self.export_section_seen
            && !self.runtime_export_added
            && matches!(
                before,
                Some(
                    SectionId::Start
                        | SectionId::Element
                        | SectionId::DataCount
                        | SectionId::Code
                        | SectionId::Data
                ) | None
            )
        {
            self.runtime_memory_export(module);
        }
        Ok(())
    }
}

fn instrument_core(bytes: &[u8], maximum_memory_bytes: f64) -> Result<Vec<u8>, String> {
    if bytes.len() > MAX_CORE_INPUT_BYTES {
        return Err("Core WebAssembly module exceeds input size limit".to_owned());
    }
    if !maximum_memory_bytes.is_finite()
        || maximum_memory_bytes.fract() != 0.0
        || maximum_memory_bytes < WASM_PAGE_BYTES as f64
        || maximum_memory_bytes > 9_007_199_254_740_991.0
    {
        return Err(
            "Component memory limit must be a safe integer of at least one Wasm page".to_owned(),
        );
    }
    let maximum_pages = ((maximum_memory_bytes as u64) / WASM_PAGE_BYTES).min(WASM32_MAX_PAGES);
    let metadata = inspect_module(bytes, maximum_pages)?;
    if metadata.type_count == u32::MAX {
        return Err("Type index overflow".to_owned());
    }

    let features = validator_features();
    Validator::new_with_features(features)
        .validate_all(bytes)
        .map_err(|error| format!("Invalid core WebAssembly module: {error}"))?;

    let mut instrumenter = Instrumenter::new(metadata, maximum_pages, bytes.len())?;
    let mut module = Module::new();
    instrumenter
        .parse_core_module(&mut module, Parser::new(0), bytes)
        .map_err(|error| error.to_string())?;
    let output = module.finish();
    if u64::try_from(output.len()).unwrap_or(u64::MAX) > MAX_CORE_OUTPUT_BYTES {
        return Err("Instrumented core module exceeds output size limit".to_owned());
    }
    Validator::new_with_features(features)
        .validate_all(&output)
        .map_err(|error| format!("Instrumented core module failed validation: {error}"))?;
    Ok(output)
}

#[cfg(target_family = "wasm")]
/// Add deadline checks and resource caps to one validated core WebAssembly module.
#[wasm_bindgen::prelude::wasm_bindgen(js_name = instrumentComponentCore)]
pub fn instrument_component_core(
    bytes: &[u8],
    maximum_memory_bytes: f64,
) -> Result<Vec<u8>, wasm_bindgen::JsValue> {
    instrument_core(bytes, maximum_memory_bytes).map_err(|error| js_sys::Error::new(&error).into())
}

#[cfg(test)]
mod tests {
    use super::{instrument_core, validator_features};
    use std::borrow::Cow;
    use wasm_encoder::{
        CodeSection, ConstExpr, CustomSection, ElementMode, ElementSection, ElementSegment,
        Elements, EntityType, ExportKind, ExportSection, Function, FunctionSection, GlobalSection,
        GlobalType, ImportSection, Instruction, MemorySection, MemoryType, Module, StartSection,
        TableSection, TableType, TypeSection, ValType,
    };
    use wasmparser::{Parser, Payload, TypeRef, Validator};

    fn tick_imports(bytes: &[u8]) -> (u32, u32) {
        for payload in Parser::new(0).parse_all(bytes) {
            if let Payload::ImportSection(section) = payload.unwrap() {
                let mut function_index = 0;
                let mut tick = None;
                for import in section.into_imports() {
                    let import = import.unwrap();
                    if matches!(import.ty, TypeRef::Func(_) | TypeRef::FuncExact(_)) {
                        if import.module == "lix:runtime/deadline" {
                            tick = Some(function_index);
                        }
                        function_index += 1;
                    }
                }
                return (function_index, tick.expect("deadline import"));
            }
        }
        panic!("runtime import section missing")
    }

    fn remapping_fixture() -> Vec<u8> {
        let mut module = Module::new();
        let mut types = TypeSection::new();
        types.ty().function([], []);
        module.section(&types);

        let mut imports = ImportSection::new();
        imports.import("env", "host", EntityType::Function(0));
        module.section(&imports);

        let mut functions = FunctionSection::new();
        functions.function(0);
        functions.function(0);
        module.section(&functions);

        let mut table = TableSection::new();
        table.table(TableType {
            element_type: wasm_encoder::RefType::FUNCREF,
            table64: false,
            minimum: 2,
            maximum: None,
            shared: false,
        });
        module.section(&table);

        let mut memories = MemorySection::new();
        memories.memory(MemoryType {
            minimum: 1,
            maximum: None,
            memory64: false,
            shared: false,
            page_size_log2: None,
        });
        module.section(&memories);

        let mut globals = GlobalSection::new();
        globals.global(
            GlobalType {
                val_type: ValType::Ref(wasm_encoder::RefType::FUNCREF),
                mutable: false,
                shared: false,
            },
            &ConstExpr::ref_func(1),
        );
        module.section(&globals);

        let mut exports = ExportSection::new();
        exports.export("run", ExportKind::Func, 2);
        module.section(&exports);
        module.section(&StartSection { function_index: 2 });

        let offset = ConstExpr::i32_const(0);
        let mut elements = ElementSection::new();
        elements.segment(ElementSegment {
            mode: ElementMode::Active {
                table: None,
                offset: &offset,
            },
            elements: Elements::Functions(Cow::Borrowed(&[1, 2])),
        });
        module.section(&elements);

        let mut code = CodeSection::new();
        let mut first = Function::new([]);
        first.instruction(&Instruction::Call(0));
        first.instruction(&Instruction::Loop(wasm_encoder::BlockType::Empty));
        first.instruction(&Instruction::Call(0));
        first.instruction(&Instruction::End);
        first.instruction(&Instruction::End);
        code.function(&first);

        let mut second = Function::new([]);
        second.instruction(&Instruction::Call(2));
        second.instruction(&Instruction::End);
        code.function(&second);
        module.section(&code);
        module.finish()
    }

    #[test]
    fn shifts_defined_function_references_and_ticks_function_and_loop_entries() {
        let output = instrument_core(&remapping_fixture(), 65_536.0).unwrap();
        let (function_import_count, tick_index) = tick_imports(&output);
        assert_eq!(function_import_count, 2);
        assert_eq!(tick_index, 1);

        let mut saw_export = false;
        let mut saw_start = false;
        let mut saw_runtime_memory = false;
        let mut body_call_indices = Vec::new();
        let mut first_body_calls = Vec::new();
        let mut saw_global_func_ref = false;
        let mut saw_element_function_refs = false;
        let mut saw_capped_memory = false;
        let mut saw_capped_table = false;
        for payload in Parser::new(0).parse_all(&output) {
            match payload.unwrap() {
                Payload::GlobalSection(section) => {
                    for global in section {
                        let mut operators = global.unwrap().init_expr.get_operators_reader();
                        if let wasmparser::Operator::RefFunc { function_index } =
                            operators.read().unwrap()
                        {
                            assert_eq!(function_index, 2);
                            saw_global_func_ref = true;
                        }
                    }
                }
                Payload::ExportSection(section) => {
                    for export in section {
                        let export = export.unwrap();
                        if export.name == "run" {
                            assert_eq!(export.index, 3);
                            saw_export = true;
                        }
                        if export.name == "__lix_runtime_memory" {
                            assert_eq!(export.kind, wasmparser::ExternalKind::Memory);
                            saw_runtime_memory = true;
                        }
                    }
                }
                Payload::StartSection { func, .. } => {
                    assert_eq!(func, 3);
                    saw_start = true;
                }
                Payload::ElementSection(section) => {
                    for element in section {
                        if let wasmparser::ElementItems::Functions(functions) =
                            element.unwrap().items
                        {
                            let indices = functions
                                .into_iter()
                                .map(Result::unwrap)
                                .collect::<Vec<_>>();
                            assert_eq!(indices, [2, 3]);
                            saw_element_function_refs = true;
                        }
                    }
                }
                Payload::MemorySection(section) => {
                    for memory in section {
                        assert_eq!(memory.unwrap().maximum, Some(1));
                        saw_capped_memory = true;
                    }
                }
                Payload::TableSection(section) => {
                    for table in section {
                        assert_eq!(table.unwrap().ty.maximum, Some(1_000_000));
                        saw_capped_table = true;
                    }
                }
                Payload::CodeSectionEntry(body) => {
                    let mut calls = Vec::new();
                    let mut operators = body.get_operators_reader().unwrap();
                    while !operators.eof() {
                        if let wasmparser::Operator::Call { function_index } =
                            operators.read().unwrap()
                        {
                            calls.push(function_index);
                        }
                    }
                    body_call_indices.push(calls.clone());
                    if body_call_indices.len() == 1 {
                        first_body_calls = calls;
                    }
                }
                _ => {}
            }
        }
        assert!(saw_export);
        assert!(saw_start);
        assert!(saw_runtime_memory);
        assert!(saw_global_func_ref);
        assert!(saw_element_function_refs);
        assert!(saw_capped_memory);
        assert!(saw_capped_table);
        assert_eq!(body_call_indices.len(), 2);
        assert_eq!(first_body_calls, [1, 0, 1, 0]);
        assert_eq!(body_call_indices[1], [1, 3]);
    }

    #[test]
    fn appends_runtime_type_after_all_types_in_a_recursion_group() {
        use wasm_encoder::{CompositeInnerType, CompositeType, FuncType, SubType};

        let subtype = || SubType {
            is_final: true,
            supertype_idx: None,
            composite_type: CompositeType {
                inner: CompositeInnerType::Func(FuncType::new([], [])),
                shared: false,
                descriptor: None,
                describes: None,
            },
        };
        let mut types = TypeSection::new();
        types.ty().rec([subtype(), subtype()]);
        let mut module = Module::new();
        module.section(&types);
        let input = module.finish();
        let output = instrument_core(&input, 65_536.0).unwrap();

        for payload in Parser::new(0).parse_all(&output) {
            if let Payload::ImportSection(section) = payload.unwrap() {
                let import = section.into_imports().last().unwrap().unwrap();
                assert_eq!(import.module, "lix:runtime/deadline");
                assert_eq!(import.ty, TypeRef::Func(2));
                return;
            }
        }
        panic!("runtime import section missing")
    }

    #[test]
    fn creates_missing_type_and_import_sections_in_canonical_order() {
        let mut input = Module::new();
        // An unrelated custom section before the first core section must not
        // affect the section ordering or survive with stale indices.
        input.section(&CustomSection {
            name: "name".into(),
            data: Cow::Borrowed(&[]),
        });
        let output = instrument_core(&input.finish(), 65_536.0).unwrap();
        let mut core_sections = Vec::new();
        let mut custom_count = 0;
        for payload in Parser::new(0).parse_all(&output) {
            match payload.unwrap() {
                Payload::TypeSection(_) => core_sections.push(1),
                Payload::ImportSection(_) => core_sections.push(2),
                Payload::CustomSection(_) => custom_count += 1,
                _ => {}
            }
        }
        assert_eq!(core_sections, [1, 2]);
        assert_eq!(custom_count, 0);
        Validator::new_with_features(validator_features())
            .validate_all(&output)
            .unwrap();
    }

    #[test]
    fn inserts_memory_inspection_export_after_exception_tags() {
        let mut input = Module::new();
        let mut types = TypeSection::new();
        types.ty().function([], []);
        input.section(&types);
        let mut memory = MemorySection::new();
        memory.memory(MemoryType {
            minimum: 1,
            maximum: Some(1),
            memory64: false,
            shared: false,
            page_size_log2: None,
        });
        input.section(&memory);
        let mut tags = wasm_encoder::TagSection::new();
        tags.tag(wasm_encoder::TagType {
            kind: wasm_encoder::TagKind::Exception,
            func_type_idx: 0,
        });
        input.section(&tags);
        let input = input.finish();
        Validator::new_with_features(validator_features())
            .validate_all(&input)
            .unwrap();
        let output = instrument_core(&input, 65_536.0).unwrap();
        let sections = Parser::new(0)
            .parse_all(&output)
            .filter_map(|payload| match payload.unwrap() {
                Payload::TagSection(_) => Some("tag"),
                Payload::ExportSection(_) => Some("export"),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(sections, ["tag", "export"]);
    }

    #[test]
    fn creates_a_type_before_an_existing_import_and_caps_imported_tables() {
        let mut input = Module::new();
        let mut imports = ImportSection::new();
        imports.import(
            "env",
            "table",
            EntityType::Table(TableType {
                element_type: wasm_encoder::RefType::FUNCREF,
                table64: false,
                minimum: 1,
                maximum: None,
                shared: false,
            }),
        );
        input.section(&imports);
        let output = instrument_core(&input.finish(), 65_536.0).unwrap();
        let mut saw_type_before_import = false;
        for payload in Parser::new(0).parse_all(&output) {
            match payload.unwrap() {
                Payload::TypeSection(_) => saw_type_before_import = true,
                Payload::ImportSection(section) => {
                    assert!(saw_type_before_import);
                    let imports = section
                        .into_imports()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    assert_eq!(imports.len(), 2);
                    assert_eq!(imports[0].module, "env");
                    assert_eq!(
                        imports[0].ty,
                        TypeRef::Table(wasmparser::TableType {
                            element_type: wasmparser::RefType::FUNCREF,
                            table64: false,
                            initial: 1,
                            maximum: Some(1_000_000),
                            shared: false,
                        })
                    );
                    assert_eq!(imports[1].module, "lix:runtime/deadline");
                    assert_eq!(imports[1].ty, TypeRef::Func(0));
                    return;
                }
                _ => {}
            }
        }
        panic!("expected the synthesized type and imports")
    }

    #[test]
    fn rejects_reserved_runtime_imports_and_unsupported_memories() {
        let mut reserved = Module::new();
        let mut types = TypeSection::new();
        types.ty().function([], []);
        reserved.section(&types);
        let mut imports = ImportSection::new();
        imports.import("lix:runtime/deadline", "tick", EntityType::Function(0));
        reserved.section(&imports);
        assert!(
            instrument_core(&reserved.finish(), 65_536.0)
                .unwrap_err()
                .contains("Reserved runtime import")
        );

        let mut shared = Module::new();
        let mut memories = MemorySection::new();
        memories.memory(MemoryType {
            minimum: 1,
            maximum: Some(1),
            memory64: false,
            shared: true,
            page_size_log2: None,
        });
        shared.section(&memories);
        assert!(instrument_core(&shared.finish(), 65_536.0).is_err());

        let mut oversized = Module::new();
        let mut memories = MemorySection::new();
        memories.memory(MemoryType {
            minimum: 2,
            maximum: None,
            memory64: false,
            shared: false,
            page_size_log2: None,
        });
        oversized.section(&memories);
        assert!(
            instrument_core(&oversized.finish(), 65_536.0)
                .unwrap_err()
                .contains("initial memory exceeds limit")
        );

        let mut conflicting_export = Module::new();
        let mut memories = MemorySection::new();
        memories.memory(MemoryType {
            minimum: 1,
            maximum: None,
            memory64: false,
            shared: false,
            page_size_log2: None,
        });
        conflicting_export.section(&memories);
        let mut exports = ExportSection::new();
        exports.export("__lix_runtime_memory", ExportKind::Memory, 0);
        conflicting_export.section(&exports);
        assert!(
            instrument_core(&conflicting_export.finish(), 65_536.0)
                .unwrap_err()
                .contains("Reserved runtime memory export")
        );

        let mut imported_memory = Module::new();
        let mut imports = ImportSection::new();
        imports.import(
            "env",
            "memory",
            EntityType::Memory(MemoryType {
                minimum: 1,
                maximum: Some(1),
                memory64: false,
                shared: false,
                page_size_log2: None,
            }),
        );
        imported_memory.section(&imports);
        assert!(
            instrument_core(&imported_memory.finish(), 65_536.0)
                .unwrap_err()
                .contains("Imported component memories")
        );

        let mut multiple_memories = Module::new();
        let mut memories = MemorySection::new();
        memories.memory(MemoryType {
            minimum: 1,
            maximum: None,
            memory64: false,
            shared: false,
            page_size_log2: None,
        });
        memories.memory(MemoryType {
            minimum: 1,
            maximum: None,
            memory64: false,
            shared: false,
            page_size_log2: None,
        });
        multiple_memories.section(&memories);
        assert!(
            instrument_core(&multiple_memories.finish(), 65_536.0)
                .unwrap_err()
                .contains("Multiple component memories")
        );
    }
}
