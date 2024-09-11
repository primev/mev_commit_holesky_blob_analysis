import marimo

__generated_with = "0.8.14"
app = marimo.App(width="full")


@app.cell
def __():
    import marimo as mo
    import altair as alt
    import polars as pl

    from lancedb_tables.lance_table import LanceTable

    pl.Config.set_fmt_str_lengths(200)
    pl.Config.set_fmt_float("full")
    return LanceTable, alt, mo, pl


@app.cell(hide_code=True)
def __(LanceTable, pl):
    # load data from tables
    commitment_table_name: str = "commitments"
    l1_tx_table_name: str = "l1_txs"
    mev_boost_table_name: str = "mev_boost_blocks"
    index: str = "block_number"
    lance_tables = LanceTable()
    uri: str = "data"  # locally saved to "data folder"

    # open the mev-commit commitments table
    commitments_table = lance_tables.open_table(
        uri=uri, table=commitment_table_name
    )

    # open the l1 txs table
    l1_tx_table = lance_tables.open_table(uri=uri, table=l1_tx_table_name)
    l1_tx_df = pl.from_arrow(l1_tx_table.to_lance().to_table())

    # open mev-boost-blocks table
    mev_boost_blocks = lance_tables.open_table(uri=uri, table=mev_boost_table_name)
    mev_boost_blocks_df = pl.from_arrow((mev_boost_blocks.to_lance().to_table()))
    return (
        commitment_table_name,
        commitments_table,
        index,
        l1_tx_df,
        l1_tx_table,
        l1_tx_table_name,
        lance_tables,
        mev_boost_blocks,
        mev_boost_blocks_df,
        mev_boost_table_name,
        uri,
    )


@app.cell(hide_code=True)
def __(commitments_table, l1_tx_df, pl):
    commit_df = (
        (
            pl.from_arrow(commitments_table.to_lance().to_table())
            .with_columns(
                (
                    pl.col("dispatchTimestamp") - pl.col("decayStartTimeStamp")
                ).alias("bid_decay_latency"),
                (pl.col("bid") / 10**18).alias("bid_eth"),
                pl.from_epoch("timestamp", time_unit="ms").alias("datetime"),
            )
            # bid decay calculations
            # the formula to calculate the bid decay = (decayEndTimeStamp - decayStartTimeStamp) / (dispatchTimestamp - decayEndTimeStamp). If it's a negative number, then bid would have decayed to 0
            .with_columns(
                # need to change type from uint to int to account for negative numbers
                pl.col("decayStartTimeStamp").cast(pl.Int64),
                pl.col("decayEndTimeStamp").cast(pl.Int64),
                pl.col("dispatchTimestamp").cast(pl.Int64),
            )
            .with_columns(
                (
                    pl.col("decayEndTimeStamp") - pl.col("decayStartTimeStamp")
                ).alias("decay_range"),
                (pl.col("decayEndTimeStamp") - pl.col("dispatchTimestamp")).alias(
                    "dispatch_range"
                ),
            )
            .with_columns(
                (pl.col("dispatch_range") / pl.col("decay_range")).alias(
                    "decay_multiplier"
                )
            )
            .with_columns(
                pl.when(pl.col("decay_multiplier") < 0)
                .then(0)
                .otherwise(pl.col("decay_multiplier"))
            )
            # calculate decayed bid. The decay multiplier is the amount that the bid decays by.
            .with_columns(
                (pl.col("decay_multiplier") * pl.col("bid_eth")).alias(
                    "decayed_bid_eth"
                )
            )
            .select(
                "datetime",
                "bid_decay_latency",
                "decay_multiplier",
                "isSlash",
                "block_number",
                "blockNumber",
                "txnHash",
                "bid_eth",
                "decayed_bid_eth",
                "commiter",
                "bidder",
            )
            .rename(
                {
                    "block_number": "mev_commit_block_number",
                    "blockNumber": "l1_block_number",
                    "txnHash": "l1_txnHash",
                }
            )
            .sort(by="datetime", descending=True)
        )
        .join(
            l1_tx_df.rename({"hash": "l1_txnHash"}),
            on="l1_txnHash",
            how="left",
            suffix="_l1",
        )
        .with_columns(
            # calculate if the preconf block was the same as the l1 block the tx ended up in
            (pl.col("block_number") - pl.col("l1_block_number")).alias(
                "l1_block_diff"
            )
        )
    ).filter(pl.col("type") == 3)
    return commit_df,


@app.cell
def __(mo):
    mo.md(r"""### mev-boost""")
    return


@app.cell(hide_code=True)
def __():
    def byte_to_string(hex_string):
        if hex_string == "0x":
            return ""
        # Remove the "0x" prefix and decode the hex string
        bytes_object = bytes.fromhex(hex_string[2:])
        try:
            human_readable_string = bytes_object.decode("utf-8")
        except UnicodeDecodeError:
            human_readable_string = bytes_object.decode("latin-1")
        return human_readable_string
    return byte_to_string,


@app.cell
def __(byte_to_string, commit_df, mev_boost_blocks_df, pl):
    mev_boost_relay_transformed_df = mev_boost_blocks_df.with_columns(
        pl.from_epoch("timestamp", time_unit="s").alias("datetime"),
        # map byte_to_string
        pl.col("extra_data")
        .map_elements(byte_to_string, return_dtype=str)
        .alias("builder_graffiti"),
        pl.when(pl.col("relay").is_null())
        .then(False)
        .otherwise(True)
        .alias("mev_boost"),
        (pl.col("value") / 10**18).round(9).alias("block_bid_eth"),
    ).select(
        "datetime",
        "block_number",
        "builder_graffiti",
        "mev_boost",
        "relay",
        "block_bid_eth",
        "base_fee_per_gas",
        "gas_used",
    )

    # transform commit_df to stsandardize to block level data
    preconf_blocks_grouped_df = (
        commit_df.select(
            "l1_block_number",
            "isSlash",
            "bid_eth",
            "decayed_bid_eth",
            "commiter",
            "bidder",
        )
        .group_by("l1_block_number")
        .agg(
            pl.col("decayed_bid_eth").sum().alias("total_decayed_bid_eth"),
            pl.col("bid_eth").sum().alias("total_bid_eth"),
            pl.col("isSlash").first().alias("isSlash"),
        )
    )
    # join mev-boost data to preconf data
    mev_boost_commit_df = mev_boost_relay_transformed_df.join(
        preconf_blocks_grouped_df,
        left_on="block_number",
        right_on="l1_block_number",
        how="left",
    ).with_columns(
        pl.when(pl.col("total_bid_eth").is_not_null())
        .then(True)
        .otherwise(False)
        .alias("preconf")
    )
    return (
        mev_boost_commit_df,
        mev_boost_relay_transformed_df,
        preconf_blocks_grouped_df,
    )


@app.cell
def __(mo):
    mo.md("""## Analysis""")
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md("""### Bid decay latency""")
    return


@app.cell
def __(commit_df):
    commit_df
    return


@app.cell
def __(alt, commit_df):
    alt.Chart(commit_df.head(2000)).mark_bar().encode(
        alt.X("bid_decay_latency:Q", bin=True),
        y='count()',
        color='isSlash'
    )
    return


@app.cell
def __():
    return


if __name__ == "__main__":
    app.run()
