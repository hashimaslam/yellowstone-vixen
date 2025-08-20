// Updated token_holders.rs to include transaction signature and SOL balance tracking

use std::borrow::Cow;
use std::collections::HashMap;
use std::str::FromStr;
use spl_pod::solana_program::example_mocks::solana_account;
use spl_token::solana_program;
use spl_token::solana_program::pubkey::Pubkey;
use yellowstone_grpc_proto::prelude::TransactionStatusMeta;
use yellowstone_vixen_core::{instruction::InstructionUpdate, TransactionUpdate, Parser, Prefilter, ParseResult, ParseError};
use crate::helpers::resolved_accounts_as_strings;
// ComputeBudget111111111111111111111111111111
#[derive(Debug, Clone)]
pub struct AccountStats {
    pub block_slot: u64,
    pub token_account: String,
    pub owner: String,
    pub mint: String,
    pub post_balance: f64,
    pub signature: String,
    pub is_off_curve: bool,
}

#[derive(Debug, Clone)]
pub struct TokenBalanceOutput {
    pub data: Vec<AccountStats>,
}

#[derive(Debug, Clone, Copy)]
pub struct TokenHoldingParser;
const COMPUTE_BUDGET_PROGRAM_ID: Pubkey = Pubkey::from_str_const("ComputeBudget111111111111111111111111111111s");

impl Parser for TokenHoldingParser {
    type Input = TransactionUpdate;
    type Output = TokenBalanceOutput;

    fn id(&self) -> Cow<str> {
        "token::TokenHoldingParser".into()
    }

    fn prefilter(&self) -> Prefilter {
        // Prefilter::builder()
        //     .transaction_accounts([])
        //     .build()
        //     .unwrap()
        Prefilter::builder()
            .transaction_accounts_include([
                solana_program::system_program::ID,
                // spl_token::ID,
                // spl_token_2022::ID,
            COMPUTE_BUDGET_PROGRAM_ID,
                solana_program::sysvar::ID,
                solana_program::bpf_loader::ID
            ])

            .build()
            .unwrap()
    }

    async fn parse(&self, input: &TransactionUpdate) -> ParseResult<Self::Output> {
        self.parse_impl(input)
    }
}

impl TokenHoldingParser {
    pub(crate) fn parse_impl(&self, tx: &TransactionUpdate) -> ParseResult<TokenBalanceOutput> {
        let mut latest_token_stats: HashMap<(String, String), AccountStats> = HashMap::new();

        if let Some(ref transaction_info) = tx.transaction {
            // Extract block slot
            let block_slot = tx.slot;
            println!("block_slot {}", block_slot);
            // Extract transaction signature
            let signature = if transaction_info.signature.is_empty() {
                format!("unknown_slot_{}", block_slot)
            } else {
                bs58::encode(&transaction_info.signature).into_string()
            };

            if let Some(ref meta) = transaction_info.meta {
                if meta.err.is_some() {
                    // Transaction failed, return empty result
                    return Ok(TokenBalanceOutput {
                        data: Vec::new(),
                    });
                }

                // Get account keys from the transaction message
                let account_keys = if let Some(ref inner_tx) = transaction_info.transaction {
                    if let Some(ref message) = inner_tx.message {
                        &message.account_keys
                    } else {
                        return Ok(TokenBalanceOutput { data: Vec::new() });
                    }
                } else {
                    return Ok(TokenBalanceOutput { data: Vec::new() });
                };

                let accounts = resolved_accounts_as_strings(tx);

                //tracing::debug!("Transaction has {} accounts, processing balances", accounts.len());

                // Process token balances
                self.update_latest_token_stats(
                    &mut latest_token_stats,
                    meta,
                    &accounts,
                    block_slot,
                    &signature,
                );

                // Process SOL balance changes
                self.process_sol_balances(
                    &mut latest_token_stats,
                    meta,
                    &accounts,
                    block_slot,
                    &signature,
                );

                //tracing::debug!("Final stats count: {}", latest_token_stats.len());
            }
        }

        Ok(TokenBalanceOutput {
            data: latest_token_stats.into_values().collect(),
        })
    }

    fn update_latest_token_stats(
        &self,
        latest_stats: &mut HashMap<(String, String), AccountStats>,
        meta: &TransactionStatusMeta,
        accounts: &[String],
        block_slot: u64,
        signature: &str,
    ) {
        // Keep track of accounts in post_token_balances
        let mut post_balance_keys: HashMap<String, bool> = HashMap::new();

        //tracing::debug!("Processing {} post_token_balances", meta.post_token_balances.len());

        // Update the stats for post_token_balances
        meta.post_token_balances.iter().for_each(|token_balance| {
            let account = &accounts[token_balance.account_index as usize];

            // Check if account is on-curve (is a wallet), but don't skip off-curve yet
            let account_pk = Pubkey::from_str(account);
            let is_on_curve = account_pk.as_ref().map_or(false, |pk| pk.is_on_curve());

            // Only process on-curve accounts (wallets)
            if !is_on_curve {
                //tracing::debug!("Skipping off-curve account: {}", account);
                return;
            }

            let key = (account.clone(), token_balance.mint.to_string());
            let ui_amount = token_balance.ui_token_amount.as_ref().map_or(0.0, |amt| amt.ui_amount);
            let owner = token_balance.owner.to_string();

            post_balance_keys.insert(account.clone(), true);

            //tracing::debug!("Adding token balance: account={}, mint={}, amount={}", account, token_balance.mint, ui_amount);

            latest_stats
                .entry(key)
                .and_modify(|stats| stats.post_balance = ui_amount)
                .or_insert_with(|| AccountStats {
                    block_slot,
                    token_account: account.clone(),
                    owner,
                    mint: token_balance.mint.to_string(),
                    post_balance: ui_amount,
                    signature: signature.to_string(),
                    is_off_curve: false,
                });
        });

        //tracing::debug!("Processing {} pre_token_balances", meta.pre_token_balances.len());

        // Set post_balance to 0 for accounts in pre_token_balances but not in post_token_balances
        for token_balance in meta.pre_token_balances.iter() {
            let account = &accounts[token_balance.account_index as usize];

            // Check if account is on-curve (is a wallet)
            let account_pk = Pubkey::from_str(account);
            let is_on_curve = account_pk.as_ref().map_or(false, |pk| pk.is_on_curve());

            if !is_on_curve {
                continue;
            }

            let key = (account.clone(), token_balance.mint.to_string());

            if !post_balance_keys.contains_key(account) {
                let owner = token_balance.owner.clone();

                //tracing::debug!("Setting token balance to 0: account={}, mint={}", account, token_balance.mint);

                latest_stats
                    .entry(key)
                    .and_modify(|stats| stats.post_balance = 0.0)
                    .or_insert_with(|| AccountStats {
                        block_slot,
                        token_account: account.clone(),
                        owner,
                        mint: token_balance.mint.to_string(),
                        post_balance: 0.0,
                        signature: signature.to_string(),
                        is_off_curve: false,
                    });
            }
        }
    }

    fn process_sol_balances(
        &self,
        latest_stats: &mut HashMap<(String, String), AccountStats>,
        meta: &TransactionStatusMeta,
        accounts: &[String],
        block_slot: u64,
        signature: &str,
    ) {
        // Create a map of pre-balances for easy lookup
        let mut pre_balances: HashMap<usize, u64> = HashMap::new();
        for (index, &balance) in meta.pre_balances.iter().enumerate() {
            pre_balances.insert(index, balance);
        }

        //tracing::debug!("Processing {} SOL balance changes", meta.post_balances.len());

        // Process post-balances and compare with pre-balances
        for (index, &post_balance) in meta.post_balances.iter().enumerate() {
            if index >= accounts.len() {
                continue; // Skip if index is out of bounds
            }

            let account = &accounts[index];

            // Check if account is on-curve (is a wallet)
            let account_pk = Pubkey::from_str(account);
            let is_on_curve = account_pk.as_ref().map_or(false, |pk| pk.is_on_curve());

            if !is_on_curve {
                continue;
            }

            let pre_balance = pre_balances.get(&index).copied().unwrap_or(0);

            let key = (account.clone(), "SOL".to_string());

            //tracing::debug!("SOL balance change: account={}, pre={}, post={}", account, pre_balance, post_balance);

            latest_stats
                .entry(key)
                .and_modify(|stats| stats.post_balance = post_balance as f64)
                .or_insert_with(|| AccountStats {
                    block_slot,
                    token_account: account.clone(),
                    owner: account.clone(), // For SOL, the account is its own owner
                    mint: "SOL".to_string(),
                    post_balance: post_balance as f64,
                    signature: signature.to_string(),
                    is_off_curve: false,
                });
        }
    }
}