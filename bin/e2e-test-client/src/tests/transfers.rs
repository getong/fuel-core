use std::fs;

use crate::test_context::{
    TestContext,
    BASE_AMOUNT,
};
use fuel_core_client::client::pagination::{
    PageDirection,
    PaginationRequest,
};
use fuel_core_types::{
    fuel_tx::{
        Address,
        AssetId,
        Input,
        Transaction,
        TransactionBuilder,
        UtxoId,
    },
    fuel_vm::SecretKey,
};
use libtest_mimic::Failed;
use tokio::time::timeout;

fn dump<T: std::fmt::Debug>(t: T, p: &str) {
    fs::write(p, format!("{:#?}", t)).unwrap();
}

async fn assert_base_asset_amount(ctx: &TestContext, address: &Address, amount: u64) {
    let base_asset_id = *ctx.alice.consensus_params.base_asset_id();
    let client = ctx.alice.client.clone();
    let balance = client
        .balances(
            address,
            PaginationRequest {
                cursor: None,
                results: 1000,
                direction: PageDirection::Forward,
            },
        )
        .await
        .unwrap()
        .results
        .first()
        .expect("should have asset")
        .amount;

    assert_eq!(balance, amount as u128);
}

fn transfer_transaction(
    from: &Address,
    secret: &SecretKey,
    utxo_id: UtxoId,
    asset_id: &AssetId,
) -> Transaction {
    let mut builder = TransactionBuilder::script(vec![], vec![]);

    const AMOUNT: u64 = 100;

    // "Response errors; Validity(InputWitnessIndexBounds { index: 0 })"
    // builder.add_input(Input::coin_signed(
    //     utxo_id,
    //     *from,
    //     AMOUNT,
    //     *asset_id,
    //     Default::default(),
    //     Default::default(),
    // ));

    // "Response errors; Transaction validity: PredicateVerificationFailed(\n    InvalidOwner,\n)" }
    // builder.add_input(Input::coin_predicate(
    // utxo_id,
    // *from,
    // AMOUNT,
    // *asset_id,
    // Default::default(),
    // 0,
    // vec![0],
    // vec![],
    // ));

    // "Response errors; The specified coin(0x00000000000000000000000000000000000000000000000000000000000000010000) doesn't exist"
    builder.add_unsigned_coin_input(
        *secret,
        utxo_id,
        AMOUNT,
        *asset_id,
        Default::default(),
    );

    builder.finalize_as_transaction()
}

pub async fn dry_run_transaction_in_the_past(ctx: &TestContext) -> Result<(), Failed> {
    const FULL_AMOUNT: u64 = 1152921504606846976;
    const HALF_AMOUNT: u64 = FULL_AMOUNT / 2;
    let base_asset_id = *ctx.alice.consensus_params.base_asset_id();

    const REQ: PaginationRequest<String> = PaginationRequest {
        cursor: None,
        results: 100,
        direction: PageDirection::Forward,
    };

    // Alice and Bob have has FULL_AMOUNT units of base asset.
    assert_base_asset_amount(ctx, &ctx.alice.address, FULL_AMOUNT).await;
    assert_base_asset_amount(ctx, &ctx.bob.address, FULL_AMOUNT).await;

    let alice_full_coin = ctx
        .alice
        .client
        .coins(&ctx.alice.address, Some(&base_asset_id), REQ)
        .await
        .unwrap()
        .results
        .first()
        .unwrap()
        .utxo_id;

    // Store the block number before the transfer.
    let height_before_transfer = ctx
        .alice
        .client
        .chain_info()
        .await
        .unwrap()
        .latest_block
        .header
        .height;

    // Alice sends HALF_AMOUNT to Bob.
    let result = ctx
        .alice
        .transfer(ctx.bob.address, HALF_AMOUNT, None)
        .await?;
    if !result.success {
        return Err("transfer failed".into())
    }
    timeout(
        ctx.config.sync_timeout(),
        ctx.bob.client.await_transaction_commit(&result.tx_id),
    )
    .await??;

    let height_after_transfer = ctx
        .alice
        .client
        .chain_info()
        .await
        .unwrap()
        .latest_block
        .header
        .height;
    assert!(
        height_after_transfer > height_before_transfer,
        "block height should increase"
    );

    // Alice has HALF_AMOUNT units of base asset, reduced by the fee (?).
    assert_base_asset_amount(ctx, &ctx.alice.address, HALF_AMOUNT - 1).await;

    // Bob has FULL_AMOUNT + HALF_AMOUNT units of base asset.
    assert_base_asset_amount(ctx, &ctx.bob.address, FULL_AMOUNT + HALF_AMOUNT).await;

    // Alice creates a transaction that uses her initial big coin and tries to dry run it
    // on top of the blockchain tip.
    let tx = transfer_transaction(
        &ctx.alice.address,
        &ctx.alice.secret,
        alice_full_coin,
        &base_asset_id,
    );
    let result = ctx
        .alice
        .client
        .dry_run_opt(&[tx], None, None, Some(height_after_transfer))
        .await;
    assert!(
        result.is_err(),
        "should not dry run transaction in the present"
    ); // TODO[RC]: Assert proper error message after figuring out how to properly add a coin.

    // Alice creates a transaction using her initial large coin and attempts to dry-run it
    // on top of the block where she still possessed the original coin.
    let tx = transfer_transaction(
        &ctx.alice.address,
        &ctx.alice.secret,
        alice_full_coin,
        &base_asset_id,
    );
    let result = ctx
        .alice
        .client
        .dry_run_opt(&[tx], None, None, Some(height_before_transfer))
        .await;
    // TODO[RC]: This should be ok ¯\_(ツ)_/¯
    // assert!(result.is_ok());
    let err = result.unwrap_err();
    assert_eq!(
        err.to_string(),
        "?",
        "should dry run transaction in the past"
    );

    Ok(())
}

// Alice makes transfer to Bob of `4 * BASE_AMOUNT` native tokens.
pub async fn basic_transfer(ctx: &TestContext) -> Result<(), Failed> {
    // alice makes transfer to bob
    let result = ctx
        .alice
        .transfer(ctx.bob.address, 4 * BASE_AMOUNT, None)
        .await?;
    if !result.success {
        return Err("transfer failed".into())
    }
    // wait until bob sees the transaction
    timeout(
        ctx.config.sync_timeout(),
        ctx.bob.client.await_transaction_commit(&result.tx_id),
    )
    .await??;
    println!("\nThe tx id of the transfer: {}", result.tx_id);

    // bob checks to see if utxo was received
    // we don't check balance in order to avoid brittleness in the case of
    // external activity on these wallets
    let received_transfer = ctx.bob.owns_coin(result.transferred_utxo).await?;
    if !received_transfer {
        return Err("Bob failed to receive transfer".into())
    }

    Ok(())
}

// Alice makes transfer to Bob of `4 * BASE_AMOUNT` native tokens - `basic_transfer`.
// Bob returns `3 * BASE_AMOUNT` tokens back to Alice and pays `BASE_AMOUNT` as a fee.
pub async fn transfer_back(ctx: &TestContext) -> Result<(), Failed> {
    basic_transfer(ctx).await?;

    // bob makes transfer to alice
    let result = ctx
        .bob
        .transfer(ctx.alice.address, 3 * BASE_AMOUNT, None)
        .await?;
    if !result.success {
        return Err("transfer failed".into())
    }
    // wait until alice sees the transaction
    timeout(
        ctx.config.sync_timeout(),
        ctx.alice.client.await_transaction_commit(&result.tx_id),
    )
    .await??;

    // alice checks to see if utxo was received
    // we don't check balance in order to avoid brittleness in the case of
    // external activity on these wallets
    let received_transfer = ctx.alice.owns_coin(result.transferred_utxo).await?;
    if !received_transfer {
        return Err("Alice failed to receive transfer".into())
    }

    Ok(())
}
