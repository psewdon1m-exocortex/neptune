# Neptune Windows UI contract

The workspace-wide Part 00 and Part 01 documents remain normative. This file records the Neptune-specific composition requested by the operator on 2026-10-05.

## Window geometry

The previous main client area was `800 × 500`, or `8:5` (`1.6:1`). The requested inverse is `500 × 800`, or `5:8` (`0.625:1`). `MainWindow` applies that exact client size after native chrome is created and keeps a `500px` minimum width so controls never collapse into horizontal scrolling.

The connection dialog is a portrait `500 × 640` form. It keeps the native Windows title bar, close behavior, password masking and owner-modal lifecycle.

## Visual mapping

- Dark working surfaces use Saturn's black and the light theme uses Saturn's
  white when palette synchronization is enabled. With it disabled, the local
  fallback keeps true black and the warm `#F3F1EA` light canvas.
- Top-level outlines use the active foreground; nested outlines and
  explanatory text use the active secondary foreground.
- Appearance has independent theme and **Synchronize colors with Saturn**
  switches. The latter applies Saturn's accent, white and black tokens while
  preserving the selected Light/Dark theme. The local accent defaults to
  `#00A8FF`; healthy and destructive states use `#62FF8C` and `#F83D3D` with text labels.
- Service and workspace names use `Space Grotesk` with the documented Segoe UI fallback. All operational text uses Consolas with the documented monospace fallbacks.
- Controls are square, at least `40px` high and expose rest, hover, keyboard-focus, pressed and disabled states.
- Hover and keyboard-focus states change color inside the arranged control
  bounds. They never scale beyond a ScrollViewer, card, footer or window edge,
  so their one-pixel outlines remain visible on every screen.
- Buttons and checkbox labels use a one-pixel text underline for hover and
  keyboard focus. Interactive states never replace the resting background;
  fields and compound list items use their accent outline instead.
- The main menu opens dedicated Mirrors, Activity and Settings screens. Each
  secondary screen has an explicit return action, following the same
  screen-oriented control model as the local Cake Proxy VPN application.
- The mapping collection owns vertical scrolling; the window has no horizontal scrollbar.
- Scrollbars are visually hidden throughout the native client. Mouse-wheel,
  touchpad, touch panning and keyboard scrolling remain available on every
  scrollable surface, including Mirrors, Activity and Settings.
- All interface copy is English-only.
- The main menu owns the profile-wide synchronization switch and confirmed
  aggregate mirror size. Every mirror entity owns an independent switch and
  its last successfully confirmed byte/file count.

## Compact native adaptation

Part 01's `250px` browser sidebar and `80px` page heading describe a full desktop web shell and do not fit a `500px` native agent window. Neptune therefore has no browser sidebar and uses a `38px` display heading. This is a deliberate compact-platform adaptation authorized by the requested portrait window, not a second web-shell standard. Information order, palette, two-level outlines, typography roles, spacing rhythm, status semantics and interaction states remain aligned with Part 01.

## Acceptance

- The Win32 client rectangle must report exactly `500 × 800` after startup.
- Main actions and connection fields must remain keyboard reachable with visible focus.
- Long profile IDs, client IDs and folder names must trim or wrap without horizontal overflow.
- Empty, configured, paused, synchronizing, cancelled and failed states retain explicit text; color is never the only state signal.
- Switching theme, Saturn palette synchronization, global synchronization or an entity switch persists across restart for the selected profile.
- `dotnet build Neptune.slnx` and the complete Core test suite must pass.
