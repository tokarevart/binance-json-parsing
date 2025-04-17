#![allow(non_snake_case)]

use std::{
    arch::x86_64::{_mm_lfence, _rdtsc},
    hint::black_box,
};

use futures_util::StreamExt;
use mimalloc::MiMalloc;
use tokio_tungstenite::connect_async;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn counter_start() -> u64 {
    unsafe {
        _mm_lfence();
        _rdtsc()
    }
}

fn counter_elapsed(start: u64) -> u64 {
    unsafe { _rdtsc() - start }
}

// {
//   "e": "bookTicker",  // event type
//   "u": 7283946601071, // order book updateId
//   "s": "BTCUSDT",     // symbol
//   "b": "83604.80",    // best bid price
//   "B": "10.746",      // best bid qty
//   "a": "83604.90",    // best ask price
//   "A": "9.514",       // best ask qty
//   "T": 1744760290967, // transaction time
//   "E": 1744760290967  // event time
// }

#[derive(serde::Deserialize, PartialEq, Eq, Debug)]
struct BookTickerOwned {
    T: u64,
    b: String,
    B: String,
    a: String,
    A: String,
}

#[derive(serde::Deserialize, PartialEq, Eq, Debug)]
struct BookTicker<'a> {
    T: u64,
    #[serde(borrow)]
    b: &'a str,
    #[serde(borrow)]
    B: &'a str,
    #[serde(borrow)]
    a: &'a str,
    #[serde(borrow)]
    A: &'a str,
}

/// All parameters here change very rarely.
#[derive(Clone, Copy, Debug)]
struct ParsingConfig {
    /// Position of `b`, as in "b":"
    start: usize,
    /// How many fractional digits are in the price values.
    price_precision: usize,
    /// How many fractional digits are in the volume values.
    volume_precision: usize,
    /// How many digits are in the transaction time.
    transaction_time_digits: usize,
}

fn parse_book_ticker(
    json: &str,
    ParsingConfig {
        start,
        price_precision,
        volume_precision,
        transaction_time_digits,
    }: ParsingConfig,
) -> BookTicker {
    assert_eq!(json.as_bytes()[start], b'b');

    // Skip 4 chars: b":"
    let b_start = start + 4;
    let dot_pos = b_start + json[b_start..].find('.').unwrap();
    let b_end = dot_pos + price_precision + 1;
    assert_eq!(json.as_bytes()[b_end], b'\"');
    let b = &json[b_start..b_end];

    // Skip 7 chars: ","B":"
    let B_start = b_end + 7;
    let dot_pos = B_start + json[B_start..].find('.').unwrap();
    let B_end = dot_pos + volume_precision + 1;
    assert_eq!(json.as_bytes()[B_end], b'\"');
    let B = &json[B_start..B_end];

    // Skip 7 chars: ","a":"
    let a_start = B_end + 7;
    let dot_pos = a_start + json[a_start..].find('.').unwrap();
    let a_end = dot_pos + price_precision + 1;
    assert_eq!(json.as_bytes()[a_end], b'\"');
    let a = &json[a_start..a_end];

    // Skip 7 chars: ","A":"
    let A_start = a_end + 7;
    let dot_pos = A_start + json[A_start..].find('.').unwrap();
    let A_end = dot_pos + volume_precision + 1;
    assert_eq!(json.as_bytes()[A_end], b'\"');
    let A = &json[A_start..A_end];

    // Skip 6 chars: ","T":
    let T_start = A_end + 6;
    // Skip 13 digits, for example, 1744848607537 will be 13 digits for a long time until this
    // parameter would have to change.
    let T_end = T_start + transaction_time_digits;
    let T = json[T_start..T_end].parse().unwrap();
    assert_eq!(json.as_bytes()[T_end], b',');

    BookTicker { T, b, B, a, A }
}

fn measure<T>(f: impl Fn() -> T) -> (u64, T) {
    let start = counter_start();
    let res = f();
    let elapsed = counter_elapsed(start);

    (elapsed, res)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = "wss://fstream.binance.com/ws/btcusdt@bookTicker";

    let (mut ws_stream, _) = connect_async(url).await?;

    let mut ticks_acc = 0;
    let mut measurements_num = 0;

    let config = ParsingConfig {
        start: 51,
        price_precision: 2,
        volume_precision: 3,
        transaction_time_digits: 13,
    };

    while let Some(msg) = ws_stream.next().await {
        let msg = msg?;
        if msg.is_text() {
            println!("{msg}");

            let text = msg.to_text().unwrap();

            // This way we find the start of the interesting part of payload.
            // So the next time we parse the payload we can just skip to this position.
            // We would just need to check that the same start position still holds the same data
            // we expect, that is, the letter `b`, otherwise we'd need to recompute the start of
            // the interesting part of payload and remember it.
            dbg!(text.find("b\":"));

            // let (elapsed, book_ticker): (u64, BookTicker) = measure(|| serde_json::from_str(text).unwrap());
            // let (elapsed, book_ticker): (u64, BookTicker) =
            //    measure(|| sonic_rs::from_str(text).unwrap());
            let (elapsed, book_ticker) = measure(|| parse_book_ticker(text, black_box(config)));

            ticks_acc += elapsed;
            measurements_num += 1;

            println!("duration now: {elapsed} ticks");
            println!("duration avg: {} ticks", ticks_acc / measurements_num);

            do_nothing(black_box(book_ticker));
            // dbg!(book_ticker);
        }
    }

    Ok(())
}

fn do_nothing<T>(_: T) {}
