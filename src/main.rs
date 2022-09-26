use std::collections::VecDeque;
use std::future::Future;
use std::task::{ Poll, Context, Waker };
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use tokio::runtime::Builder;

// struct in queue has waker + output value option

struct SnowflakeGenState {
    queue: VecDeque<Arc<Mutex<SnowflakeState>>>, 
    waker: Option<Waker>
}

#[derive(Clone)]
struct SnowflakeGen {
    state: Arc<Mutex<SnowflakeGenState>>
}

impl SnowflakeGen {
    fn new() -> Self {
        let state = Arc::new(Mutex::new(SnowflakeGenState {
            queue: VecDeque::new(),
            waker: None
        }));
        SnowflakeGen { state }
    }

    fn gen_loop(&self) -> SnowflakeGenLoop {
        SnowflakeGenLoop::new(self.state.clone())
    }

    fn gen(&mut self) -> Snowflake {
        // TODO: locking to push to queue is kind of wack.
        let mut state = self.state.lock().unwrap();
        if state.queue.len() == 0 {
            if let Some(waker) = &state.waker {
                let waker_clone = waker.clone();
                waker_clone.wake();
            }
        }
        Snowflake::new(&mut state)
    }
}

struct SnowflakeGenLoop {
    state: Arc<Mutex<SnowflakeGenState>>
}

impl SnowflakeGenLoop {
    fn new(state: Arc<Mutex<SnowflakeGenState>>) -> Self {
        Self { state }
    }
}

impl Future for SnowflakeGenLoop {
    type Output = ();
    
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.lock().unwrap();
        
        // Generation logic
        while let Some(snowflake_lock) = state.queue.pop_front() {
            let mut snowflake_state = snowflake_lock.lock().unwrap();
            snowflake_state.output = Some(1);
            if let Some(waker) = &snowflake_state.waker {
                let waker_clone = waker.clone();
                waker_clone.wake();
            }
        }

        state.waker = Some(cx.waker().clone());
        cx.waker().clone().wake();
        Poll::Pending
    }
}

struct SnowflakeState {
    output: Option<u64>,
    waker: Option<Waker>
}

struct Snowflake {
    state: Arc<Mutex<SnowflakeState>>
}

impl Snowflake {
    fn new(gen_state: &mut SnowflakeGenState) -> Self {
        let state = Arc::new(Mutex::new(SnowflakeState {
            output: None,
            waker: None
        }));
        gen_state.queue.push_back(state.clone());
        Self { state: state.clone() }
    }
}

impl Future for Snowflake {
    type Output = u64;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.lock().unwrap();

        if let Some(output) = state.output {
            Poll::Ready(output)
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

async fn gen_bunch(mut gen: SnowflakeGen) {
    let s1 = gen.gen().await;
    println!("{}", s1);
    let s1 = gen.gen().await;
    println!("{}", s1);
    let s1 = gen.gen().await;
    println!("{}", s1);
    let s1 = gen.gen().await;
    println!("{}", s1);
}

fn main() {
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let gen = SnowflakeGen::new();
    runtime.spawn(gen_bunch(gen.clone()));
    runtime.block_on(gen.gen_loop());
}