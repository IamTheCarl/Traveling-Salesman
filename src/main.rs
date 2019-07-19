
extern crate csv;
extern crate num_cpus;
extern crate byteorder;

use std::fs::File;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::Arc;
use core::fmt;
use std::fmt::Formatter;
use std::cmp::Ordering;
use crossbeam::thread;
use crossbeam_channel::unbounded;
use core::cmp;
use std::io::{BufWriter, Write};
use crossbeam_channel::bounded;
use std::fs::OpenOptions;
use byteorder::{ReadBytesExt, WriteBytesExt, NativeEndian};
use std::io::SeekFrom;
use std::io::Seek;
use std::path::Path;
use std::collections::vec_deque::VecDeque;
use crossbeam_channel::Receiver;

const CSV_FILE: &'static str = "dataset.csv";

struct StateNode {
    name: String,
    id: u8, // We use integers here so we can do the compares and lookups faster.
    connections: Vec<StateConnection>
}

#[derive(Clone)]
struct StateConnection {
    distance: u32,
    target: u8
}

#[derive(Clone)]
struct TravelPath {
    state_map: Arc<HashMap<u8, StateNode>>,
    states: Vec<u8>,
    length: u32
}

impl TravelPath {
    fn get_end(&self) -> u8 {
        self.states[self.states.len() - 1]
    }

    fn get_count_of_state(&self, state_to_count: u8) -> u32 {
        let mut count = 0;

        for state in self.states.iter() {
            if *state == state_to_count {
                count += 1;
            }
        }

        count
    }

    fn has_a_double_repeat(&self) -> bool {
        let mut states = self.states.clone();
        states.sort();
        states.dedup();

        let mut num_dups = 0;

        for state in states {
            if self.get_count_of_state(state) > 1 {
                num_dups += 1;
                if num_dups >= 2 {
                    return true;
                }
            }
        }

        false
    }
}

impl fmt::Display for TravelPath {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "states {}, length {} ", self.states.len(), self.length)?;

        for state in self.states.iter() {
            let state = self.state_map.get(state).unwrap();

            write!(f, "{}->", state.name)?;
        }

        write!(f, "END")
    }
}

impl PartialOrd for TravelPath {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(other.cmp(self))
    }
}

impl Ord for TravelPath {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        let count_a = self.get_count_of_state(self.get_end());
        let count_b = other.get_count_of_state(other.get_end());

        let freq = count_b.cmp(&count_a);

        if freq == Ordering::Less {
            Ordering::Greater
        } else {
            self.length.cmp(&other.length)
        }
    }
}

impl PartialEq for TravelPath {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl std::cmp::Eq for TravelPath {

}

struct PathStack {
    file: File,
    count: usize,
    state_map: Arc<HashMap<u8, StateNode>>,
    cache: VecDeque<TravelPath>
}

impl PathStack {
    fn new(state_map: &Arc<HashMap<u8, StateNode>>, path: &Path) -> Self {

        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .expect("Unable to open path stack file");

        Self {
            file,
            count: 0,
            state_map: Arc::clone(state_map),
            cache: VecDeque::new()
        }
    }

    fn push(&mut self, path: TravelPath) {
        self.cache.push_front(path);

        let mut len = self.cache.len();

        if len > 1000000 {
            print!("Storing memory to hard drive...");

            while len > 500000 {
                let to_store = self.cache.pop_back().unwrap();
                self.push_to_file(to_store);

                len = self.cache.len();
            }

            self.cache.shrink_to_fit();

            println!("Done.");
        }
    }

    fn push_to_file(&mut self, path: TravelPath) {
        // println!("PUSH: {}", path);

        self.file.write_u32::<NativeEndian>(path.length).unwrap();

        for state in path.states.iter() {
            self.file.write_u8(*state).unwrap();
        }

        self.file.write_u16::<NativeEndian>(path.states.len() as u16).unwrap();

        self.count += 1;
    }

    fn pop(&mut self) -> Option<TravelPath> {
        let result = self.cache.pop_front();

        let mut len = self.cache.len();
        if len < 2000 {
            let mut read_message = false;

            while len < 50000 {
                let from_file = self.pop_from_file();
                match from_file {
                    Some(path) => {
                        if !read_message {
                            print!("Reading memory from hard drive...");
                            read_message = true;
                        }

                        self.cache.push_back(path)
                    },
                    None => break
                };
                len = self.cache.len();
            }

            if read_message {
                println!("Done.");
            }
        }

        result
    }

    fn pop_from_file(&mut self) -> Option<TravelPath> {
        if self.count > 0 {
            let mut file = &self.file;

            let mut states = Vec::new();

            file.seek(SeekFrom::Current(-2)).unwrap();
            let num_states = file.read_u16::<NativeEndian>().unwrap();
            file.seek(SeekFrom::Current(-(num_states as i64) - 2)).unwrap();

            for _n in 0..num_states {
                let state = file.read_u8().unwrap();
                states.push(state);
            }

            file.seek(SeekFrom::Current(-(num_states as i64) - 4)).unwrap();
            let length = file.read_u32::<NativeEndian>().unwrap();
            file.seek(SeekFrom::Current(-4)).unwrap();

            self.count -= 1;

            let path = TravelPath {
                state_map: Arc::clone(&self.state_map),
                states,
                length
            };

            // println!("POP: {}", path);

            Some(path)

        } else {
            None
        }
    }
}

fn load_states(path: &str) -> Result<HashMap<u8, StateNode>, std::io::Error> {
    use std::io::Error;
    use std::io::ErrorKind;

    let mut map = HashMap::new();
    let mut state_name_map = HashMap::new();
    let mut next_state_index: u8 = 0;

    let file = File::open(path)?;
    let mut rdr = csv::Reader::from_reader(file);

    for result in rdr.records() {
        let record = result?;
        let state_name = record.get(0).ok_or(Error::new(
            ErrorKind::InvalidData, "Missing source state identity.`"))?;

        let distance = record.get(1)
            .ok_or(Error::new(ErrorKind::InvalidData, "Missing distance.`"))?.parse();

        let distance= distance.unwrap_or(0);

        let target_name = record.get(2).ok_or(Error::new(
            ErrorKind::InvalidData, "Missing state target.`"))?;

        let state_index = *state_name_map.entry(String::from(state_name))
            .or_insert({
                let index = next_state_index;
                next_state_index += 1;
                index
            });

        let target_index = *state_name_map.entry(String::from(target_name))
            .or_insert({
                let old_index = next_state_index;
                next_state_index += 1;
                old_index
            });

        println!("From {} to {} by {}.", state_name, target_name, distance);

        // First make sure our target exists.
        let target = map.entry(target_index)
            .or_insert(StateNode {
                name: String::from(target_name),
                id: target_index,
                connections: Vec::new()
            });

        target.connections.push(StateConnection {
            distance: distance, target: state_index
        });

        // Now we maybe create ourselves and then add our target.
        let state = map.entry(state_index)
            .or_insert(StateNode {
                name: String::from(state_name),
                id: state_index,
                connections: Vec::new()
            });

        state.connections.push(StateConnection {
            distance: distance, target: target_index
        });
    }

    Ok(map)
}

fn get_start_and_end(states: &HashMap<u8, StateNode>) -> (&StateNode, &StateNode) {
    let mut start_state: Option<&StateNode> = None;
    let mut end_state: Option<&StateNode> = None;

    for (_index, state) in states.iter() {
        if state.name == "WA" {
            start_state = Some(state);

            if end_state.is_some() {
                break;
            }
        }

        if state.name == "ME" {
            end_state = Some(state);

            if start_state.is_some() {
                break;
            }
        }
    }

    let start_state = start_state.expect("Starting point California could not be found.");
    let end_state = end_state.expect("Ending point Maine could not be found.");

    (start_state, end_state)
}

fn main() {
    let num_threads = num_cpus::get();
    // let num_threads = 1;

    let state_map = Arc::new(load_states(CSV_FILE).unwrap());

    let (start_state, end_state) = get_start_and_end(&state_map);

    // Can use RAM or hard drive. This thing eats up RAM fast so I use the hard drive.
    let stack = Arc::new(Mutex::new(PathStack::new(&state_map, Path::new("paths_stack"))));
    // let stack = Arc::new(Mutex::new(Vec::new()));

    //let (path_sender, paths) = unbounded();
    let (path_sender, path_processor_receiver) = bounded(5000);
    let (path_processor_sender, path_receiver) = bounded(5000);
    let (log_sender, log_receiver) = bounded(5);
    let (solution_sender, solution_receiver) = bounded(0);

    stack.lock().unwrap().push(TravelPath {
        state_map: Arc::clone(&state_map),
        states: vec![start_state.id],
        length: 0
    });

    thread::scope(|s| {

        let path_processor_sender = path_processor_sender.clone();
        let path_processor_receiver = path_processor_receiver.clone();

        {
            let stack = Arc::clone(&stack);

            s.spawn(move |_| {
                println!("Collection thread started.");

                for path in path_processor_receiver {
                    stack.lock().unwrap().push(path);
                }

                println!("Collection thread finished.");
            });
        }

        {
            let stack = Arc::clone(&stack);

            s.spawn(move |_| {
                println!("Distribution thread started.");

                loop {
                    let path = stack.lock().unwrap().pop();
                    match path {
                        Some(path) => {
                            path_processor_sender.send(path).unwrap();
                        },
                        None => {
                            // TODO shutdown the application?
                            // break;
                        }
                    }
                }

                println!("Distribution thread finished.");
            });
        }

        let solution_receiver: Receiver<TravelPath> = solution_receiver.clone();

        // Acquisition thread.
        s.spawn(move |_| {
            println!("Acquisition thread started.");

            let file = File::create("./solutions.txt").unwrap();
            let mut file_writer = BufWriter::new(&file);

            //let stdout = std::io::stdout();
            //let mut stdout_writer = stdout.lock();

            let mut shortest: u32 = 0xFFFFFFFF;

            for path in solution_receiver {
                if path.length < shortest {
                    shortest = path.length;

                    writeln!(file_writer, "Solution: {}", path).unwrap();
                    file_writer.flush().unwrap();
                    println!("Solution: {}", path);
                }
            }

            println!("Acquisition thread finished.");
        });

        let log_receiver = log_receiver.clone();

        // Logger thread.
        s.spawn(move |_| {
            println!("Logger thread started.");

            let file = File::create("./loud_log.txt").unwrap();
            let mut writer = BufWriter::new(&file);

            let mut line_count = 0;

            for path in log_receiver {
                if line_count >= 100000 {
                    writeln!(writer, "Failed path: {}", path).unwrap();
                    // println!("Failed path: {}", path);

                    writer.flush().unwrap();
                    line_count = 0;
                } else {
                    line_count += 1;
                }
            }

            println!("Logger thread finished.");
        });

        // We spawn one thread per core.
        for i in 0..num_threads {

            let path_receiver = path_receiver.clone();
            let path_sender = path_sender.clone();
            let log_sender = log_sender.clone();
            let solution_sender = solution_sender.clone();
            let state_map = Arc::clone(&state_map);

            s.spawn(move |_| {
                println!("Start worker thread {}.", i);

                for path in path_receiver {
                    // log_sender.send(path.clone()).unwrap();
                    // solution_sender.send(path.clone()).unwrap();

                    if path.get_end() != end_state.id {
                        // println!("Path: {}", path);
                        if !log_sender.is_full() {
                            log_sender.send(path.clone()).unwrap();
                        }

                        let connections = &state_map.get(&path.get_end()).unwrap().connections;

                        let mut new_searches = Vec::new();

                        for dest in connections.iter() {
                            let state = dest.target;
                            let length = path.length + dest.distance;

                            // Not a repeat state. We're good to go through here.
                            new_searches.push(TravelPath {
                                state_map: Arc::clone(&state_map),
                                states: {
                                    let mut val = path.states.clone();
                                    val.push(state);
                                    val
                                },
                                length: length
                            });

                            new_searches.retain(|x| {
                                x.get_count_of_state(x.get_end()) <= 2
                                    && !x.has_a_double_repeat()
                            });

                            new_searches.sort();
                        }

                        for path in new_searches {
                            path_sender.send(path).unwrap();
                        }
                    } else {
                        // This could be the end but we need to check that all states have been visited.

                        // Definatly possible.
                        let mut good = true;

                        // Each state must be in the path at least once.
                        for (id, _node) in state_map.iter() {
                            if !path.states.contains(id) {
                                good = false;
                                break;
                            }
                        }

                        if good {
                            solution_sender.send(path).unwrap();
                        }
                    }
                }

                println!("End thread {}.", i);
            });
        }
    }).unwrap();
}

#[cfg(test)]
mod testing {
    use super::*;

    #[test]
    fn load_csv() {
        // Should be 41 states.

        let states = load_states(CSV_FILE).unwrap();

        for (_index, state) in states.iter() {
            println!("Found state: {}", state.name);
        }

        assert_eq!(states.len(), 42, "Wrong number of states were loaded.");
    }
}