using System.Windows;
using System.Windows.Controls;
using Neptune.Core;
using Button = System.Windows.Controls.Button;
using TextBox = System.Windows.Controls.TextBox;
using ListBox = System.Windows.Controls.ListBox;
using ComboBox = System.Windows.Controls.ComboBox;
using CheckBox = System.Windows.Controls.CheckBox;

namespace Neptune.Windows;
public partial class MainWindow
{
    private async void ReviewMapping_Click(object sender,RoutedEventArgs e)
    {
        if(sender is not Button{Tag:MappingViewModel item}||_ownedSyncService is null)return;
        try
        {
            if(_ownedSyncService.SyncInProgress)
            {
                var status=new TextBox{IsReadOnly=true,AcceptsReturn=true,Margin=new Thickness(20),VerticalScrollBarVisibility=ScrollBarVisibility.Auto};
                var live=new Window{Owner=this,Title="Synchronization · "+item.DisplayName,Width=700,Height=420,Content=status};
                var timer=new System.Windows.Threading.DispatcherTimer{Interval=TimeSpan.FromSeconds(1)};
                async Task Refresh(){var values=await _ownedSyncService.ReadDiagnosticsAsync();if(values.TryGetValue(item.Mapping.MappingId,out var current))status.Text=PipelineStatus(current)+"\n"+string.Join(Environment.NewLine,current.Phases.Where(p=>p.Calls>0||p.Active>0).Select(p=>$"{p.Phase}: active={p.Active}, current={p.RunningMilliseconds} ms, completed={p.Calls}, SQL commands={p.SqlCommands}, items/bytes={p.Items}"));}
                timer.Tick+=async(_,_)=>await Refresh();live.Closed+=(_,_)=>timer.Stop();await Refresh();timer.Start();live.Show();return;
            }
            var id=item.Mapping.MappingId;var review=await _ownedSyncService.ReviewMappingAsync(id);
            var panel=new StackPanel{Margin=new Thickness(20)};
            var dialog=new Window{Owner=this,Title="Review · "+item.DisplayName,Width=720,Height=720,WindowStartupLocation=WindowStartupLocation.CenterOwner,Content=new ScrollViewer{Content=panel,VerticalScrollBarVisibility=ScrollBarVisibility.Auto}};
            void Label(string text)=>panel.Children.Add(new TextBlock{Text=text,TextWrapping=TextWrapping.Wrap,Margin=new Thickness(0,8,0,8)});
            Label($"Queued journal records: {review.Diagnostics.InboxRecords}; journal lag: {review.Diagnostics.JournalLag}; retries: {review.Diagnostics.Retries}. "+review.Diagnostics.WaitReason);
            Label(PipelineStatus(review.Diagnostics));
            Label("Conflicts (first 120). Re-observe after correcting the source, or explicitly exclude a path to preserve its remote copy.");
            var conflicts=new ListBox{Height=140,ItemsSource=review.Conflicts,DisplayMemberPath=nameof(LocalParentReconciliationConflict.RelativePath)};panel.Children.Add(conflicts);
            var reason=new TextBlock{TextWrapping=TextWrapping.Wrap};panel.Children.Add(reason);conflicts.SelectionChanged+=(_,_)=>reason.Text=(conflicts.SelectedItem as LocalParentReconciliationConflict)?.Reason??"";
            var again=new Button{Content="Re-observe mapping",Margin=new Thickness(0,6,0,6)};panel.Children.Add(again);again.Click+=(_,_)=>{ScheduleHintSync();dialog.Close();};
            Label($"Quarantine preview: {review.Deletes.Operations.Count} proven absences in this page (up to 500). Approval applies to this exact page; changed evidence or additional pages require review.");
            if(review.Deletes.Failures.Count>0)Label(string.Join(Environment.NewLine,review.Deletes.Failures.Take(20).Select(f=>$"{f.Kind} · {f.Path} · {f.State} · {f.Error}")));
            var delete=new Button{Content="Review and approve quarantine",IsEnabled=review.Deletes.Operations.Count>0};panel.Children.Add(delete);
            delete.Click+=async(_,_)=>
            {
                var text=string.Join(Environment.NewLine,review.Deletes.Operations.Take(40).Select(x=>x.Path))+Environment.NewLine+$"Total: {review.Deletes.Operations.Count}\nPlan: {review.Deletes.Digest}";
                if(MessageBox.Show(dialog,text,"Approve quarantine plan",MessageBoxButton.YesNo,MessageBoxImage.Warning)!=MessageBoxResult.Yes)return;
                try{await _ownedSyncService.ApproveDeletesAsync(id,review.Deletes.Digest);delete.IsEnabled=false;ScheduleHintSync();}
                catch(Exception error){MessageBox.Show(dialog,error.Message,"Plan needs attention");}
            };
            Label("Choose excluded folders from this mirror's Exclude… button. Existing local and remote files are preserved.");
            var exclusions = new Button { Content = "Edit excluded folders…", Tag = item };
            panel.Children.Add(exclusions);
            exclusions.Click += (button, args) => { dialog.Close(); ExcludedFolders_Click(button, args); };
            var batches=new CheckBox{Content="Enable negotiated small-file batches for this mapping",IsChecked=_preferences.SmallBatchMappings.Contains(id,StringComparer.Ordinal),Margin=new Thickness(0,12,0,0)};panel.Children.Add(batches);
            batches.Click+=(_,_)=>{try{var ids=_preferences.SmallBatchMappings.Where(x=>x!=id).ToList();if(batches.IsChecked==true)ids.Add(id);var next=_preferences with{SmallBatchMappings=ids.ToArray()};_preferencesStore.Write(next);_preferences=next;}catch(Exception error){MessageBox.Show(dialog,error.Message,"Batch preference");}};
            dialog.ShowDialog();
        }
        catch(Exception error){MessageBox.Show(this,error.Message,"Mapping review",MessageBoxButton.OK,MessageBoxImage.Information);}
    }
    private static string PipelineStatus(SyncDiagnosticSnapshot value)
        =>$"Captured: {value.CapturedBytes} bytes; received: {value.ReceivedBytes} bytes; published: {value.PublishedFiles} files.\n"+
        $"Backend written/verified bytes: not reported. Journal inbox: {value.InboxRecords}; lag: {value.JournalLag}.\n"+
        $"Pending operations: {value.Schedule?.Waiting??0}; oldest: {(value.Schedule?.OldestPendingMilliseconds??0)/1000} s; state: {value.Schedule?.State??value.WaitReason??"ready"}.\n"+
        $"Shared workers: {value.Scheduler?.ActiveData??0}/{value.Scheduler?.DataLimit??4}; queued: {value.Scheduler?.Queued??0}; resource wait: {value.Scheduler?.PoolWaitMilliseconds??0} ms.";
}
