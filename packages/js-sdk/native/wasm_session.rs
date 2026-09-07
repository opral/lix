//! One exported method list and one set of JS conversions for both WASM modes.
//! Each wrapper supplies `inner: impl SessionOperations` and telemetry wrapping.

macro_rules! wasm_session_methods {
    ($wrapper:ty) => {
        #[wasm_bindgen::prelude::wasm_bindgen]
        impl $wrapper {
            #[wasm_bindgen(js_name = execute)]
            pub async fn execute(
                &self,
                sql: String,
                params: JsValue,
                options: Option<JsValue>,
            ) -> Result<JsValue, JsValue> {
                let params = crate::wasm::values_from_js(params)?;
                let options = crate::wasm::session_execute_options_from_js(options)?;
                let result = self
                    .instrument_operation(crate::session::SessionOperations::execute(
                        &self.inner,
                        &sql,
                        &params,
                        options,
                    ))
                    .await
                    .map_err(crate::wasm::lix_error_to_js)?;
                crate::wasm::execute_result_to_js(result)
            }

            #[wasm_bindgen(js_name = executeBatch)]
            pub async fn execute_batch(
                &self,
                statements: JsValue,
                options: Option<JsValue>,
            ) -> Result<JsValue, JsValue> {
                let statements = crate::wasm::batch_statements_from_js(statements)?;
                let options = crate::wasm::session_execute_options_from_js(options)?;
                let results = self
                    .instrument_operation(crate::session::SessionOperations::execute_batch(
                        &self.inner,
                        &statements,
                        options,
                    ))
                    .await
                    .map_err(crate::wasm::lix_error_to_js)?;
                let results = results
                    .into_iter()
                    .map(crate::wasm::ExecuteResultDto::try_from)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(crate::wasm::lix_error_to_js)?;
                crate::wasm::to_js(&results)
            }

            #[wasm_bindgen(js_name = activeBranchId)]
            pub async fn active_branch_id(&self) -> Result<String, JsValue> {
                self.instrument_operation(crate::session::SessionOperations::active_branch_id(
                    &self.inner,
                ))
                .await
                .map_err(crate::wasm::lix_error_to_js)
            }

            #[wasm_bindgen(js_name = activeAccountId)]
            pub async fn active_account_id(&self) -> Result<String, JsValue> {
                self.instrument_operation(crate::session::SessionOperations::active_account_id(
                    &self.inner,
                ))
                .await
                .map_err(crate::wasm::lix_error_to_js)
            }

            #[wasm_bindgen(js_name = createBranch)]
            pub async fn create_branch(&self, options: JsValue) -> Result<JsValue, JsValue> {
                let options: crate::wasm::CreateBranchOptionsDto = crate::wasm::from_js(options)?;
                let receipt = self
                    .instrument_operation(crate::session::SessionOperations::create_branch(
                        &self.inner,
                        lix::CreateBranchOptions {
                            id: options.id,
                            name: options.name,
                            from_commit_id: options.from_commit_id,
                        },
                    ))
                    .await
                    .map_err(crate::wasm::lix_error_to_js)?;
                crate::wasm::to_js(&crate::wasm::CreateBranchReceiptDto {
                    id: receipt.id,
                    name: receipt.name,
                    hidden: receipt.hidden,
                    commit_id: receipt.commit_id,
                })
            }

            #[wasm_bindgen(js_name = undo)]
            pub async fn undo(&self) -> Result<JsValue, JsValue> {
                let receipt = self
                    .instrument_operation(crate::session::SessionOperations::undo(&self.inner))
                    .await
                    .map_err(crate::wasm::lix_error_to_js)?;
                crate::wasm::to_js(&crate::wasm::UndoReceiptDto {
                    branch_id: receipt.branch_id,
                    target_commit_id: receipt.target_commit_id,
                    inverse_commit_id: receipt.inverse_commit_id,
                })
            }

            #[wasm_bindgen(js_name = redo)]
            pub async fn redo(&self) -> Result<JsValue, JsValue> {
                let receipt = self
                    .instrument_operation(crate::session::SessionOperations::redo(&self.inner))
                    .await
                    .map_err(crate::wasm::lix_error_to_js)?;
                crate::wasm::to_js(&crate::wasm::RedoReceiptDto {
                    branch_id: receipt.branch_id,
                    target_commit_id: receipt.target_commit_id,
                    replay_commit_id: receipt.replay_commit_id,
                })
            }

            #[wasm_bindgen(js_name = switchBranch)]
            pub async fn switch_branch(&self, options: JsValue) -> Result<JsValue, JsValue> {
                let options: crate::wasm::SwitchBranchOptionsDto = crate::wasm::from_js(options)?;
                let receipt = self
                    .instrument_operation(crate::session::SessionOperations::switch_branch(
                        &self.inner,
                        lix::SwitchBranchOptions {
                            branch_id: options.branch_id,
                        },
                    ))
                    .await
                    .map_err(crate::wasm::lix_error_to_js)?;
                crate::wasm::to_js(&crate::wasm::SwitchBranchReceiptDto {
                    branch_id: receipt.branch_id,
                })
            }

            #[wasm_bindgen(js_name = mergeBranchPreview)]
            pub async fn merge_branch_preview(&self, options: JsValue) -> Result<JsValue, JsValue> {
                let options: crate::wasm::MergeBranchOptionsDto = crate::wasm::from_js(options)?;
                let preview = self
                    .instrument_operation(crate::session::SessionOperations::merge_branch_preview(
                        &self.inner,
                        lix::MergeBranchPreviewOptions {
                            source_branch_id: options.source_branch_id,
                        },
                    ))
                    .await
                    .map_err(crate::wasm::lix_error_to_js)?;
                crate::wasm::to_js(&crate::wasm::MergeBranchPreviewDto::from(preview))
            }

            #[wasm_bindgen(js_name = mergeBranch)]
            pub async fn merge_branch(&self, options: JsValue) -> Result<JsValue, JsValue> {
                let options: crate::wasm::MergeBranchOptionsDto = crate::wasm::from_js(options)?;
                let receipt = self
                    .instrument_operation(crate::session::SessionOperations::merge_branch(
                        &self.inner,
                        lix::MergeBranchOptions {
                            source_branch_id: options.source_branch_id,
                        },
                    ))
                    .await
                    .map_err(crate::wasm::lix_error_to_js)?;
                crate::wasm::to_js(&crate::wasm::MergeBranchReceiptDto::from(receipt))
            }
        }
    };
}

pub(super) use wasm_session_methods;
