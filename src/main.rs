use std::future::Future;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::task::{Poll, Context, Waker};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Instant, UNIX_EPOCH, SystemTime};

use tokio::runtime::Builder;

struct SnowflakeGenState {
    rx: Receiver<Arc<Mutex<SnowflakeState>>>,
    waker: Option<Waker>
}

#[derive(Clone)]
struct SnowflakeGen {
    tx: Sender<Arc<Mutex<SnowflakeState>>>,
    state: Arc<Mutex<SnowflakeGenState>>
}

impl SnowflakeGen {
    fn new() -> Self {
        let (tx, rx) = channel();
        let state = Arc::new(Mutex::new(SnowflakeGenState {
            rx,
            waker: None
        }));
        SnowflakeGen {
            tx,
            state
        }
    }

    fn gen_loop(&self) -> SnowflakeGenLoop {
        SnowflakeGenLoop::new(self)
    }

    fn gen(&mut self) -> Snowflake {
        Snowflake::gen(self)
    }
}

struct SnowflakeGenLoop {
    incr_size: u32,
    incr_max: u32,

    time_offset: u64,
    epoch: Instant,

    // Generator state.
    past: Option<Instant>,
    incr: u32,
    incr_offset: Option<u32>,

    state: Arc<Mutex<SnowflakeGenState>>
}

impl SnowflakeGenLoop {
    fn new(gen: &SnowflakeGen) -> Self {
        let time_offset = {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
            (now - 1640995200000).try_into().unwrap()
            //     ^^^^^^^^^^^^^ This magic number is the SnowflakeGen's epoch. This should be
            //                   specified in SnowflakeGen::new and retrieved from gen.
        };

        let incr_size = 10;

        Self {
            incr_size,
            incr_max: 2u32.pow(incr_size),

            time_offset,
            epoch: Instant::now(),

            past: None,
            incr: 0,
            incr_offset: None,

            state: gen.state.clone()
        }
    }
}

impl Future for SnowflakeGenLoop {
    type Output = ();
    
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state_clone = self.state.clone();
        let mut state = state_clone.lock().unwrap();

        state.waker = Some(cx.waker().clone());
        cx.waker().clone().wake();

        // Generation logic.
        let now = Instant::now();

        match self.past {
            Some(past) => if now.duration_since(past).as_millis() > 0 {
                self.incr_offset = Some(match self.incr_offset {
                    Some(incr_offset) => (incr_offset + self.incr) % self.incr_max,
                    None => self.incr
                });
                self.incr = 0;
                self.past = Some(now);
            },
            None => self.past = Some(now)
        }

        if self.incr >= self.incr_max {
            return Poll::Pending;
        }
        
        let time_sum = self.time_offset + u64::try_from(now.duration_since(self.epoch).as_millis()).unwrap();

        for snowflake_lock in state.rx.iter() {
            let mut snowflake_state = snowflake_lock.lock().unwrap();

            let incr_sum = match self.incr_offset {
                Some(incr_offset) => (incr_offset + self.incr) % self.incr_max,
                None => self.incr
            };
            let snowflake = (time_sum << self.incr_size) + u64::from(incr_sum);

            snowflake_state.output = Some(snowflake);
            if let Some(waker) = &snowflake_state.waker {
                let waker_clone = waker.clone();
                waker_clone.wake();
            }

            self.incr += 1;
            if self.incr >= self.incr_max {
                break;
            }
        }

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
    fn gen(gen: &mut SnowflakeGen) -> Self {
        let state = Arc::new(Mutex::new(SnowflakeState {
            output: None,
            waker: None
        }));

        gen.tx.send(state.clone()).unwrap();

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
    for _ in 0..100000 {
        let snowflake = gen.gen().await;
        println!("{}", snowflake);
    }
}

fn main() {
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let gen = SnowflakeGen::new();
    runtime.spawn(gen.gen_loop());
    runtime.block_on(gen_bunch(gen.clone()));
}
