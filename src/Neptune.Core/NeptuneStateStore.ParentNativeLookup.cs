namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private static string ParentNativeIdentitySql(string columns)
        // Probe both indexed identities instead of applying COALESCE to every
        // managed row. A remapped logical key must not match its old native ID.
        // CROSS JOIN keeps the native-identity probe first without ANALYZE.
        =>"SELECT "+columns+" FROM local_cycle_files f WHERE f.scope_id=$scope AND f.node_id=$node AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_incarnations i WHERE i.scope_id=f.scope_id AND i.node_id=f.node_id)"
          +" UNION ALL SELECT "+columns+" FROM local_parent_reconcile_incarnations i CROSS JOIN local_cycle_files f ON f.scope_id=i.scope_id AND f.node_id=i.node_id WHERE i.scope_id=$scope AND i.native_id=$node";
}
