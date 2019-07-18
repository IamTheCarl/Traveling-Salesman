
extern crate csv;
extern crate crossbeam;
extern crate num_cpus;

use std::fs::File;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::mpsc;
use crossbeam::thread;
use std::sync::Arc;
use core::fmt;
use std::fmt::Formatter;
use std::cmp::Ordering;

const CSV_FILE: &'static str = "dataset.csv";

struct StateNode {
    name: String,
    id: u32, // We use integers here so we can do the compares and lookups faster.
    connections: Vec<StateConnection>
}

#[derive(Clone)]
struct StateConnection {
    distance: u32,
    target: u32
}

struct Path {
    state_map: Arc<HashMap<u32, StateNode>>,
    states: Vec<u32>,
    length: u32
}

impl Path {
    fn get_end(&self) -> u32 {
        self.states[self.states.len() - 1]
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        for state in self.states.iter() {
            let state = self.state_map.get(state).unwrap();

            write!(f, "{}->", state.name)?;
        }

        write!(f, "END, states {}, length {}", self.states.len(), self.length )
    }
}

fn load_states(path: &str) -> Result<HashMap<u32, StateNode>, std::io::Error> {
    use std::io::Error;
    use std::io::ErrorKind;

    let mut map = HashMap::new();
    let mut state_name_map = HashMap::new();
    let mut next_state_index = 0;

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

fn get_start_and_end(states: &HashMap<u32, StateNode>) -> (&StateNode, &StateNode) {
    let mut california: Option<&StateNode> = None;
    let mut maine: Option<&StateNode> = None;

    for (_index, state) in states.iter() {
        if state.name == "WA" {
            california = Some(state);

            if maine.is_some() {
                break;
            }
        }

        if state.name == "ME" {
            maine = Some(state);

            if california.is_some() {
                break;
            }
        }
    }

    let california = california.expect("Starting point California could not be found.");
    let maine = maine.expect("Ending point Maine could not be found.");

    (california, maine)
}

fn main() {
    let state_map = Arc::new(load_states(CSV_FILE).unwrap());

    //let mut paths = Vec::new();
    let queue = Arc::new(Mutex::new(Vec::new()));

    let (california, maine) = get_start_and_end(&state_map);
    // We start our journey in California.
    queue.lock().unwrap().push(Path {
        state_map: state_map.clone(),
        states: vec![california.id],
        length: 0
    });

    // Used to safely store found paths.
    let (out, path_found) = mpsc::channel();

    let num_threads = num_cpus::get();
    // let num_threads = 4;

    // This is the length of a random path I manually calculated.
    // let shortest_path_length = Arc::new(Mutex::new(13911));
    let shortest_path_length = Arc::new(Mutex::new(0xFFFFFFFF));


    // We spawn one thread per core.
    thread::scope(|s| {
        for i in 0..num_threads {
            let queue = Arc::clone(&queue);
            let state_map = Arc::clone(&state_map);
            let shortest_path_length = Arc::clone(&shortest_path_length);
            let out = out.clone();
            s.spawn(move |_| {
                println!("Spawn thread {}.", i);
                loop {
                    // We pop from the front to keep memory usage low.
                    let mut queue_copy = Vec::new();
                    queue_copy.append(&mut queue.lock().unwrap());

                    let path = queue_copy.pop();
                    match path {
                        Some(path) => {
                            let states = &path.states;
                            let end_index = *states.get(states.len() - 1).unwrap();
                            let end = state_map.get(&end_index).unwrap();

                            // Another path to go down.
                            if end_index != maine.id {
                                println!("CHECK: {}", path);
                                let mut new_searches = Vec::new();
                                let shortest_length = shortest_path_length.lock().unwrap();

                                // We want to try states we haven't tried yet first, so we're going to sort this.
                                fn get_count(states: &Vec<u32>, state_to_count: u32) -> u32 {
                                    let mut count = 0;

                                    for state in states {
                                        if *state == state_to_count {
                                            count += 1;
                                        }
                                    }

                                    count
                                }

                                let mut connections = end.connections.clone();
                                // println!("NUM CONNECTIONS: {}", connections.len());

                                connections.retain(|x| {
                                    get_count(states, x.target) <= 2
                                });

                                connections.sort_by(|a, b| {
                                    let count_a = get_count(states, a.target);
                                    let count_b = get_count(states, b.target);

                                    // println!("{} count: {} distance: {}", state_map.get(&a.target).unwrap().name, count_a, a.distance);

                                    let freq = count_b.cmp(&count_a);

                                    if freq != Ordering::Equal {
                                        freq
                                    } else {
                                        b.distance.cmp(&a.distance)
                                    }
                                });

                                for con in connections.iter() {
                                    let count_a = get_count(states, con.target);
                                    println!("{} count: {} distance: {}", state_map.get(&con.target).unwrap().name, count_a, con.distance);
                                }

                                for dest in connections.iter() {
                                    let state = dest.target;
                                    let length = path.length + dest.distance;

                                    // Not a repeat state. We're good to go through here.
                                    if length < *shortest_length {
                                        new_searches.push(Path {
                                            state_map: state_map.clone(),
                                            states: {
                                                let mut val = states.clone();
                                                val.push(state);
                                                val
                                            },
                                            length
                                        });
                                    }
                                }

                                // Keep this locked for as short a time as possible.
                                if !new_searches.is_empty() {
                                    let mut queue = queue.lock().unwrap();
                                    queue.append(&mut new_searches);
                                }
                            } else { // We could have a path here.
                                // Definatly possible.
                                if path.states.len() >= state_map.len() {
                                    let mut good = true;

                                    // Each state must be in the path at least once.
                                    for (id, _node) in state_map.iter() {
                                        if !path.states.contains(id) {
                                            good = false;
                                            // println!("REJECT: {}\nMISSING: {}", path, _node.name);
                                            break;
                                        }
                                    }

                                    // This is a valid path.
                                    if good {
                                        out.send(path).unwrap();
                                    }

                                }
                            }
                        },
                        None => break
                    }

                    std::thread::sleep(std::time::Duration::new(1, 0));
                }

                println!("End thread {}.", i);
            });
        }

        let shortest_path_length = Arc::clone(&shortest_path_length);

        // Acquisition thread.
        s.spawn(move |_| {
            println!("Acquisition thread started.");

            for path in path_found {
                println!("FOUND: {}", path);
                let mut shortest_length = shortest_path_length.lock().unwrap();

                if path.length < *shortest_length {
                    *shortest_length = path.length;
                }
            }

            println!("Aquisition thread finished.");
        });
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