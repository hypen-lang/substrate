#![allow(dead_code)]
use substrate_liquid::*;
pub fn seed(db: &Database) -> Id {
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(demo::core(), Visibility::Private)
        .unwrap();
    let person = tx.insert("core.Person", vec!["Alice".into()]).unwrap();
    for app in ["fitness", "crm"] {
        tx.grant_schema(app, app).unwrap();
        tx.grant(
            app,
            "core.Person",
            Grant {
                read: true,
                reference: true,
                ..Grant::read_only()
            },
        )
        .unwrap();
    }
    tx.commit().unwrap();
    db.session("fitness")
        .transaction()
        .install_and_commit(demo::fitness());
    db.session("crm")
        .transaction()
        .install_and_commit(demo::crm());
    person
}
trait InstallCommit {
    fn install_and_commit(self, module: Module);
}
impl InstallCommit for Transaction {
    fn install_and_commit(mut self, module: Module) {
        self.install_with_visibility(module, Visibility::Private)
            .unwrap();
        self.commit().unwrap();
    }
}
pub fn fixture() -> (Database, Id) {
    let db = Database::memory();
    let id = seed(&db);
    (db, id)
}
pub fn workout(tx: &mut Transaction, person: Id, minutes: i64) -> (Id, Id) {
    let workout = tx.insert("fitness.Workout", vec![minutes.into()]).unwrap();
    let performed = tx
        .insert("fitness.Performed", vec![person.into(), workout.into()])
        .unwrap();
    (workout, performed)
}
pub fn deal(tx: &mut Transaction, person: Id, amount: i64) -> (Id, Id) {
    let deal = tx.insert("crm.Deal", vec![Value::Decimal(amount)]).unwrap();
    let linked = tx
        .insert("crm.CustomerDeal", vec![person.into(), deal.into()])
        .unwrap();
    (deal, linked)
}
