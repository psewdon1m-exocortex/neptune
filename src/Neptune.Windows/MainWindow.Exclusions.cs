using System.Collections.ObjectModel;
using System.Windows;
using System.Windows.Controls;
using Microsoft.Win32;
using Button = System.Windows.Controls.Button;
using ListBox = System.Windows.Controls.ListBox;
using Orientation = System.Windows.Controls.Orientation;
using Control = System.Windows.Controls.Control;

namespace Neptune.Windows;

public partial class MainWindow
{
    private async void ExcludedFolders_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button { Tag: MappingViewModel item } || _ownedSyncService is null) return;
        try
        {
            FooterStatus.Text = "Reading folder exclusions; waiting for the current sync cycle if necessary…";
            var saved = await _ownedSyncService.ReadExcludedFoldersAsync(item.Mapping);
            FooterStatus.Text = "Folder exclusions";
            var paths = new ObservableCollection<string>(saved);
            var panel = new Grid { Margin = new Thickness(20) };
            foreach (var height in new[] { GridLength.Auto, GridLength.Auto, new GridLength(1, GridUnitType.Star), GridLength.Auto, GridLength.Auto, GridLength.Auto })
                panel.RowDefinitions.Add(new RowDefinition { Height = height });
            var dialog = new Window
            {
                Owner = this, Title = "Excluded folders · " + item.DisplayName, Width = 560, Height = 480,
                MinWidth = 480, MinHeight = 400, WindowStartupLocation = WindowStartupLocation.CenterOwner,
                Content = panel, Background = Background, Foreground = Foreground
            };
            dialog.SetResourceReference(FontFamilyProperty, "InterfaceFont");
            void Add(UIElement element, int row) { Grid.SetRow(element, row); panel.Children.Add(element); }
            Add(new TextBlock { Text = item.LocalPath, TextWrapping = TextWrapping.Wrap, FontWeight = FontWeights.Bold, Margin = new Thickness(0, 0, 0, 12) }, 0);
            Add(new TextBlock
            {
                Text = "Folders in this list and all their contents are excluded from this mirror. Existing local and server files are kept. Removing an exclusion resumes normal synchronization.",
                TextWrapping = TextWrapping.Wrap, Margin = new Thickness(0, 0, 0, 12)
            }, 1);
            var list = new ListBox { ItemsSource = paths, MinHeight = 100, Margin = new Thickness(0, 0, 0, 8), HorizontalContentAlignment = System.Windows.HorizontalAlignment.Stretch };
            list.SetResourceReference(Control.BackgroundProperty, "BlackBrush");
            list.SetResourceReference(Control.ForegroundProperty, "WhiteBrush");
            list.SetResourceReference(Control.BorderBrushProperty, "White80Brush");
            ScrollViewer.SetHorizontalScrollBarVisibility(list, ScrollBarVisibility.Disabled);
            ScrollViewer.SetVerticalScrollBarVisibility(list, ScrollBarVisibility.Hidden);
            var pathText = new FrameworkElementFactory(typeof(TextBlock));
            pathText.SetBinding(TextBlock.TextProperty, new System.Windows.Data.Binding());
            pathText.SetValue(TextBlock.TextWrappingProperty, TextWrapping.Wrap);
            list.ItemTemplate = new DataTemplate { VisualTree = pathText };
            System.Windows.Automation.AutomationProperties.SetName(list, "Excluded folders");
            Add(list, 2);
            var actions = new StackPanel { Orientation = Orientation.Horizontal };
            var add = new Button { Content = "Add folder…", MinWidth = 120 };
            var remove = new Button { Content = "Remove exclusion", IsEnabled = false, Margin = new Thickness(8, 0, 0, 0) };
            actions.Children.Add(add); actions.Children.Add(remove); Add(actions, 3);
            var status = new TextBlock { TextWrapping = TextWrapping.Wrap, Margin = new Thickness(0, 12, 0, 12), Text = paths.Count == 0 ? "No folders are excluded." : $"Excluded folders: {paths.Count}" };
            Add(status, 4);
            var footer = new StackPanel { Orientation = Orientation.Horizontal, HorizontalAlignment = System.Windows.HorizontalAlignment.Right };
            var save = new Button { Content = "Save", MinWidth = 90 };
            var cancel = new Button { Content = "Cancel", MinWidth = 90, IsCancel = true, Margin = new Thickness(8, 0, 0, 0) };
            footer.Children.Add(save); footer.Children.Add(cancel); Add(footer, 5);
            list.SelectionChanged += (_, _) => remove.IsEnabled = list.SelectedItem is string;
            add.Click += (_, _) =>
            {
                var picker = new OpenFolderDialog { Title = "Choose a subfolder to exclude", InitialDirectory = item.LocalPath, Multiselect = true };
                if (picker.ShowDialog(dialog) != true) return;
                try
                {
                    var additions = picker.FolderNames.Select(folder => WindowsFolderExclusions.SelectedFolder(item.LocalPath, folder));
                    var normalized = WindowsFolderExclusions.Normalize(paths.Concat(additions));
                    paths.Clear(); foreach (var path in normalized) paths.Add(path);
                    status.Text = $"Excluded folders: {paths.Count}. Save to apply.";
                }
                catch (Exception error) { status.Text = error.Message; }
            };
            remove.Click += (_, _) => { if (list.SelectedItem is string path) paths.Remove(path); status.Text = "Save to apply. Previously excluded files will be checked again."; };
            using var lifetime = new CancellationTokenSource();
            dialog.Closed += (_, _) => lifetime.Cancel();
            save.Click += async (_, _) =>
            {
                save.IsEnabled = add.IsEnabled = remove.IsEnabled = list.IsEnabled = false;
                status.Text = "Saving; waiting for the current sync cycle if necessary…";
                try
                {
                    var result = await _ownedSyncService.SetExcludedFoldersAsync(item.Mapping, paths.ToArray(), saved, lifetime.Token);
                    saved = paths.ToArray();
                    RefreshWatchers();
                    if (result.Applied) { ScheduleHintSync(); dialog.DialogResult = true; }
                    else status.Text = "Saved, but synchronization is paused for this mapping: " + result.PendingReason
                        + " Restore the previous exclusions to finish pending work before changing them. No original files were removed.";
                }
                catch (OperationCanceledException) when (lifetime.IsCancellationRequested) { }
                catch (Exception error) { status.Text = error.Message; }
                finally { save.IsEnabled = add.IsEnabled = list.IsEnabled = true; remove.IsEnabled = list.SelectedItem is string; }
            };
            cancel.Click += (_, _) => dialog.Close();
            dialog.ShowDialog();
        }
        catch (Exception error) { MessageBox.Show(this, error.Message, "Folder exclusions", MessageBoxButton.OK, MessageBoxImage.Information); }
    }
}
