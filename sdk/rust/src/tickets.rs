//! The listener's connection tickets (spec/link.md, "Connection tickets"),
//! in memory. A set is the tickets of one connectInfo answer, one per
//! candidate, for one device and connection id. Times are the wall
//! clock's, which a sleep moves too: a ticket is good for a minute of real
//! time.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::wire::link::{
    MAX_TICKET_SETS_PER_DEVICE, MAX_TICKETS, RETIRED_TICKETS_KEPT, TICKET_TTL_MS, make_link_ticket,
};
use crate::wire::relay::CandidateKind;

#[derive(Clone)]
struct Ticket {
    /// The device and connection id it was minted for.
    set: (String, String),
    kind: CandidateKind,
    expires_at: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Check {
    Pending {
        device_id: String,
        connection_id: String,
    },
    /// Issued here, but it opens nothing now: spent, expired, retired, or
    /// for the other kind of arrival.
    Stale,
    /// Never issued here: a guess.
    Unknown,
}

#[derive(Default)]
pub(crate) struct LinkTickets {
    pending: HashMap<String, Ticket>,
    /// Spent, and still in their sets, so one spent at a hello the listener
    /// could not finish is good again while its set is.
    spent: HashMap<String, Ticket>,
    /// Each set's tickets, oldest set first.
    sets: Vec<((String, String), Vec<String>)>,
    /// Oldest first.
    retired: VecDeque<String>,
    retired_set: HashSet<String>,
}

impl LinkTickets {
    /// One ticket per kind, in order, or none past the cap.
    pub fn mint(
        &mut self,
        device_id: &str,
        connection_id: &str,
        kinds: &[CandidateKind],
        now: u64,
    ) -> Option<Vec<String>> {
        let set = (device_id.to_owned(), connection_id.to_owned());
        self.drop_set(&set);
        let own: Vec<_> = self.sets.iter().filter(|(key, _)| key.0 == device_id).map(|(key, _)| key.clone()).collect();
        let over = (own.len() + 1).saturating_sub(MAX_TICKET_SETS_PER_DEVICE);
        for oldest in &own[..over] {
            self.drop_set(oldest);
        }
        let expired: Vec<String> = self
            .pending
            .iter()
            .filter(|(_, entry)| entry.expires_at <= now)
            .map(|(ticket, _)| ticket.clone())
            .collect();
        for ticket in expired {
            self.forget(&ticket);
        }
        if self.pending.len() + kinds.len() > MAX_TICKETS {
            return None;
        }
        let tickets: Vec<String> = kinds
            .iter()
            .map(|kind| {
                let ticket = make_link_ticket();
                let entry = Ticket { set: set.clone(), kind: *kind, expires_at: now + TICKET_TTL_MS };
                self.pending.insert(ticket.clone(), entry);
                ticket
            })
            .collect();
        self.sets.push((set, tickets.clone()));
        Some(tickets)
    }

    pub fn check(&self, ticket: &str, kind: CandidateKind, now: u64) -> Check {
        match self.pending.get(ticket) {
            None if self.retired_set.contains(ticket) => Check::Stale,
            None => Check::Unknown,
            Some(entry) if entry.kind != kind || entry.expires_at <= now => Check::Stale,
            Some(entry) => Check::Pending { device_id: entry.set.0.clone(), connection_id: entry.set.1.clone() },
        }
    }

    /// Spends the ticket, whether or not it was still good.
    pub fn spend(&mut self, ticket: &str, kind: CandidateKind, now: u64) -> bool {
        let Some(entry) = self.pending.get(ticket).filter(|entry| entry.kind == kind).cloned() else {
            return false;
        };
        self.retire(ticket);
        let good = entry.expires_at > now;
        self.spent.insert(ticket.to_owned(), entry);
        good
    }

    /// A spent ticket good again, unless its set was retired since.
    pub fn unspend(&mut self, ticket: &str) {
        let Some(entry) = self.spent.remove(ticket) else { return };
        self.retired_set.remove(ticket);
        self.retired.retain(|other| other != ticket);
        self.pending.insert(ticket.to_owned(), entry);
    }

    /// Drops the tickets of every device not in `device_ids`.
    pub fn keep_devices(&mut self, device_ids: &HashSet<String>) {
        let gone: Vec<_> =
            self.sets.iter().filter(|(key, _)| !device_ids.contains(&key.0)).map(|(key, _)| key.clone()).collect();
        for set in gone {
            self.drop_set(&set);
        }
    }

    fn drop_set(&mut self, set: &(String, String)) {
        let Some(index) = self.sets.iter().position(|(key, _)| key == set) else { return };
        let (_, tickets) = self.sets.remove(index);
        for ticket in tickets {
            self.retire(&ticket);
        }
    }

    fn forget(&mut self, ticket: &str) {
        let entry = self.pending.get(ticket).cloned();
        self.retire(ticket);
        let Some(entry) = entry else { return };
        let Some(index) = self.sets.iter().position(|(key, _)| *key == entry.set) else { return };
        self.sets[index].1.retain(|other| other != ticket);
        if self.sets[index].1.is_empty() {
            self.sets.remove(index);
        }
    }

    fn retire(&mut self, ticket: &str) {
        self.pending.remove(ticket);
        self.spent.remove(ticket);
        if self.retired_set.insert(ticket.to_owned()) {
            self.retired.push_back(ticket.to_owned());
        }
        while self.retired.len() > RETIRED_TICKETS_KEPT {
            if let Some(oldest) = self.retired.pop_front() {
                self.retired_set.remove(&oldest);
            }
        }
    }
}
