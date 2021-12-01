use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io;
use std::sync::mpsc::{channel, Receiver, Sender};

type PageId = u64;
type TxId = u64;
type Page = ();

struct Tx<'a> {
    cache: &'a mut Cache,
    txid: TxId,
    workspace: Workspace,
}

#[derive(Default)]
struct Workspace;

struct Cc {}

struct StabilityInfo {}

struct Cache {
    residents: BTreeMap<(PageId, TxId), Page>,
    cc: Cc,
    stability_info: Receiver<StabilityInfo>,
    next_txid: u64,
}

impl Cache {
    fn tx(&mut self) -> Tx {
        let txid = self.next_txid;
        self.next_txid += 1;
        Tx {
            cache: self,
            txid,
            workspace: Default::default(),
        }
    }

    fn get(&mut self, pid: PageId, txid: TxId) -> io::Result<Page> {
        todo!()
    }

    fn commit(&mut self, workspace: Workspace) -> io::Result<bool> {
        todo!()
    }
}

struct Log {
    active_file: File,
    stabilizing_file: File,
}

struct Dw {
    active_file: File,
    stabiizing_file: File,
}

struct ArrReader(File);

struct ArrWriter(File);

fn read_page(file: &File, pid: PageId) -> io::Result<()> {
    todo!()
}

fn write_page(file: &File, pid: PageId, page: &Page) -> io::Result<()> {
    todo!()
}
