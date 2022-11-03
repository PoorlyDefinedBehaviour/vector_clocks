use core::num;
use std::collections::HashMap;

/// A [vector clock][WikipediaVectorClock] is a data structure used for determining the partial ordering
/// of events in a distributed system.
///
/// [WikipediaVectorClock]: https://en.wikipedia.org/wiki/Vector_clock
#[derive(Debug, Clone, PartialEq)]
struct VectorClock {
  /// The process id is the index of the clock in `clocks` that belongs to the process.
  process_id: usize,
  /// List of logical clocks. Each clock belongs to a process that's part of the system.
  clocks: Vec<usize>,
}

impl VectorClock {
  fn new(process_id: usize, processes: usize) -> Self {
    assert!(
      process_id < processes,
      "process id must be the index of the clock that belongs to the process"
    );

    Self {
      process_id,
      clocks: vec![0; processes],
    }
  }

  fn increment_this_process_clock(&mut self) {
    self.increment_process_clock(self.process_id);
  }

  fn increment_process_clock(&mut self, process_id: usize) {
    self.clocks[process_id] += 1;
  }

  fn message_received(&mut self, other_process_vector_clock: &VectorClock) {
    self.increment_this_process_clock();

    for (this_process_value, other_process_value) in self
      .clocks
      .iter_mut()
      .zip(other_process_vector_clock.clocks.iter())
    {
      *this_process_value = std::cmp::max(*this_process_value, *other_process_value);
    }
  }
}

#[derive(Debug)]
struct Message {
  vector_clock: VectorClock,
  key: String,
  value: String,
}

#[derive(Debug, PartialEq, Clone)]
struct KvEntry {
  vector_clock: VectorClock,
  value: String,
}

#[derive(Debug)]
struct Process {
  process_id: usize,
  num_processes: usize,
  kv: HashMap<String, KvEntry>,
}

impl Process {
  fn new(process_id: usize, num_processes: usize) -> Self {
    Self {
      process_id,
      num_processes,
      kv: HashMap::new(),
    }
  }

  fn get(&self, key: &str) -> Option<&KvEntry> {
    self.kv.get(key)
  }

  fn receive_message(&mut self, message: Message) {
    if let Some(entry) = self.kv.get_mut(&message.key) {
      entry.vector_clock.message_received(&message.vector_clock);
      entry.value = message.value;
    } else {
      let mut entry = KvEntry {
        vector_clock: VectorClock::new(self.process_id, self.num_processes),
        value: message.value,
      };
      entry.vector_clock.message_received(&message.vector_clock);
      self.kv.insert(message.key, entry);
    }
  }
}

fn get(key: &str, p1: &Process, p2: &Process) {
  let v1 = p1.get(key).unwrap();
  let v2 = p2.get(key).unwrap();

  if v1
    .vector_clock
    .clocks
    .iter()
    .zip(v2.vector_clock.clocks.iter())
    .all(|(p1_clock, p2_clock)| p1_clock <= p2_clock)
  {
    println!(
      "ok. p1={:?} p2={:?}",
      v1.vector_clock.clocks, v2.vector_clock.clocks,
    );
  } else {
    println!(
      "conflicting versions: p1={:?} p2={:?}",
      v1.vector_clock.clocks, v2.vector_clock.clocks,
    );
  }
}

fn main() {
  conflict_example_1();
  no_conflict_example_1();
}

fn conflict_example_1() {
  const NUM_PROCESSES: usize = 3;

  let mut p1 = Process::new(0, NUM_PROCESSES);
  let mut p2 = Process::new(1, NUM_PROCESSES);

  let key = "key1".to_owned();

  p1.receive_message(Message {
    vector_clock: VectorClock::new(0, NUM_PROCESSES),
    key: key.clone(),
    value: "value 1".to_owned(),
  });

  let p1_entry = p1.get(&key).cloned().unwrap();

  p1.receive_message(Message {
    vector_clock: p1_entry.vector_clock,
    key: key.clone(),
    value: "value 2".to_owned(),
  });

  let p1_entry = p1.get(&key).cloned().unwrap();

  p1.receive_message(Message {
    vector_clock: p1_entry.vector_clock.clone(),
    key: key.clone(),
    value: "value 3".to_owned(),
  });

  p2.receive_message(Message {
    vector_clock: p1_entry.vector_clock,
    key: key.clone(),
    value: "value 3".to_owned(),
  });

  get(&key, &p1, &p2);
}

fn no_conflict_example_1() {
  const NUM_PROCESSES: usize = 3;

  let mut p1 = Process::new(0, NUM_PROCESSES);
  let mut p2 = Process::new(1, NUM_PROCESSES);

  let key = "key1".to_owned();

  p1.receive_message(Message {
    vector_clock: VectorClock::new(0, NUM_PROCESSES),
    key: key.clone(),
    value: "value 1".to_owned(),
  });

  let p1_entry = p1.get(&key).cloned().unwrap();

  p1.receive_message(Message {
    vector_clock: p1_entry.vector_clock,
    key: key.clone(),
    value: "value 2".to_owned(),
  });

  let p1_entry = p1.get(&key).cloned().unwrap();

  p2.receive_message(Message {
    vector_clock: p1_entry.vector_clock,
    key: key.clone(),
    value: "value 3".to_owned(),
  });

  get(&key, &p1, &p2);
}
