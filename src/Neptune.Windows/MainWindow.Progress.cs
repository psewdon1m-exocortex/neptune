using System.Windows;
using System.Windows.Threading;
using Neptune.Core;

namespace Neptune.Windows;

public partial class MainWindow
{
    private readonly WindowsSyncProgressDisplay _progressDisplay=new();
    private readonly DispatcherTimer _progressTimer=new(){Interval=TimeSpan.FromMilliseconds(500)};
    private WindowsSyncProgress? _currentProgress;
    private long _progressGeneration;
    private void InitializeProgress()
    {
        _progressTimer.Tick+=(_,_)=>{if(_currentProgress is { } progress)RenderProgress(progress);};
    }
    private long BeginProgress()
    {
        _progressDisplay.Begin();
        RenderProgress(new(null,0,0,new(SyncProgressStage.Preparing)));
        _progressTimer.Start();return ++_progressGeneration;
    }
    private void EndProgress(SyncProgressStage stage)
    {
        ++_progressGeneration;_progressTimer.Stop();_progressDisplay.Stop();
        RenderProgress(new(null,0,0,new(stage)));
    }
    private void RenderProgress(WindowsSyncProgress progress)
    {
        _currentProgress=progress;var display=_progressDisplay.Update(progress);
        SyncProgressPanel.Visibility=Visibility.Visible;
        SyncStageText.Text=display.Title;SyncStageText.ToolTip=display.Title;
        SyncPercentText.Text=display.PercentText;SyncCountText.Text=display.Detail;SyncEtaText.Text=display.Eta;
        SyncElapsedText.Text=display.Elapsed;
        SyncProgressBar.IsIndeterminate=display.Indeterminate;SyncProgressBar.Value=display.Percent??0;
        System.Windows.Automation.AutomationProperties.SetHelpText(SyncProgressBar,$"{display.Title}. {display.Detail}. {display.Elapsed}. {display.Eta}");
    }
}
