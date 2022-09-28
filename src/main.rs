use std::future::Future;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::task::{Poll, Context, Waker};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Instant, UNIX_EPOCH, SystemTime};

use tokio::runtime::Builder;

const INCR_SIZE: u32 = 10;
const INCR_MAX: u32 = 2u32.pow(INCR_SIZE);

struct SnowflakeGenInnerState {
    time_offset: u64,
    epoch: Instant,

    past: Option<Instant>,
    incr_offset: Option<u32>,
    incr: u32,
}

struct SnowflakeGenState {
    len: Arc<AtomicUsize>,
    rx: Receiver<Arc<Mutex<SnowflakeState>>>,
    
    waker: Option<Waker>,

    inner_state: Arc<Mutex<SnowflakeGenInnerState>>
}

impl SnowflakeGenInnerState {
    fn time_sum(&mut self) -> u64 {
        let now = Instant::now();

        match self.past {
            Some(past) => if now.duration_since(past).as_millis() > 0 {
                self.incr_offset = Some(match self.incr_offset {
                    Some(incr_offset) => (incr_offset + self.incr) % INCR_MAX,
                    None => self.incr
                });
                self.incr = 0;
                self.past = Some(now);
            },

            None => self.past = Some(now)
        }

        let time_term = u64::try_from(now.duration_since(self.epoch).as_millis()).unwrap();
        let time_sum = self.time_offset + time_term;

        time_sum
    }

    fn gen(&mut self, time_sum: u64) -> u64 {
        assert!(self.incr < INCR_MAX);

        let incr_sum = match self.incr_offset {
            Some(incr_offset) => (incr_offset + self.incr) % INCR_MAX,
            None => self.incr
        };
        let snowflake = (time_sum << INCR_SIZE) + u64::from(incr_sum);
        self.incr += 1;

        snowflake
    }
}

#[derive(Clone)]
struct SnowflakeGen {
    len: Arc<AtomicUsize>,
    tx: Sender<Arc<Mutex<SnowflakeState>>>,
    state: Arc<Mutex<SnowflakeGenState>>
}

impl SnowflakeGen {
    fn new(epoch: u64) -> Self {
        let len = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = channel();
        let time_offset = {
            let unix_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
            (unix_timestamp - u128::from(epoch)).try_into().unwrap()
        };
        let inner_state = SnowflakeGenInnerState {
            time_offset,
            epoch: Instant::now(),

            past: None,
            incr_offset: None,
            incr: 0
        };
        let state = Arc::new(Mutex::new(SnowflakeGenState {
            len: len.clone(),
            rx,

            waker: None,

            inner_state: Arc::new(Mutex::new(inner_state))
        }));
        SnowflakeGen {
            len: len.clone(),
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
    state: Arc<Mutex<SnowflakeGenState>>
}

impl SnowflakeGenLoop {
    fn new(gen: &SnowflakeGen) -> Self {
        Self { state: gen.state.clone() }
    }
}

impl Future for SnowflakeGenLoop {
    type Output = ();
    
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state_clone = self.state.clone();
        let mut state = state_clone.lock().unwrap();

        state.waker = Some(cx.waker().clone());
        cx.waker().clone().wake();

        let inner_state_clone = state.inner_state.clone();
        let mut inner_state = inner_state_clone.lock().unwrap();
        let time_sum = inner_state.time_sum();

        if inner_state.incr >= INCR_MAX {
            return Poll::Pending;
        }

        for snowflake_lock in state.rx.try_iter() {
            state.len.fetch_sub(1, Ordering::AcqRel);

            let mut snowflake_state = snowflake_lock.lock().unwrap();
            
            let snowflake = inner_state.gen(time_sum);

            snowflake_state.output = Some(snowflake);
            if let Some(waker) = &snowflake_state.waker {
                let waker_clone = waker.clone();
                waker_clone.wake();
            }

            if inner_state.incr >= INCR_MAX {
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
        let len = gen.len.load(Ordering::Acquire);
        if len == 0 {
            let gen_state = gen.state.lock().unwrap();
            let mut inner_state = gen_state.inner_state.lock().unwrap();
            let time_sum = inner_state.time_sum();

            if inner_state.incr < INCR_MAX {
                // Bypass queue.
                let output = Some(inner_state.gen(time_sum));
                let state = Arc::new(Mutex::new(SnowflakeState {
                    output,
                    waker: None
                }));
                return Self { state };
            }
        }

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
    for _ in 0..1_000_000 {
        let snowflake = gen.gen().await;
        println!("{}", snowflake);
    }
}

fn main() {
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let gen = SnowflakeGen::new(1640995200000);
    runtime.spawn(gen.gen_loop());
    runtime.block_on(gen_bunch(gen.clone()));
}
