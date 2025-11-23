# Date/Time Range Selection Feature

## Overview

This branch adds configurable date/time range selection to the Amazon Seller Central scraper. The feature allows you to filter metrics by specific date and time ranges using the dashboard's "Customised" tab.

## What's New

### Added Features

1. **Three Date Range Modes**:
   - `today` - Filter to current day
   - `relative` - Go back N days from today
   - `custom` - Specify exact date/time ranges

2. **Time Range Support**: Configure both dates AND times for precise filtering

3. **Backward Compatible**: Feature is opt-in (disabled by default)

4. **Robust Error Handling**: Gracefully falls back to default view if date picker unavailable

### Code Changes

- Added helper functions for UI interaction:
  - `_find_customised_tab()` - Locates the Customised tab with multiple selector fallbacks
  - `_wait_for_date_picker()` - Waits for date picker widget to appear
  - `get_date_time_range_from_config()` - Calculates dates/times from config
  - `apply_date_time_range()` - Applies the date/time filter

- Modified `process_single_store()` to apply date filters before scraping metrics

- Comprehensive logging at each step for debugging

## Configuration Examples

### Example 1: Today Only

```json
{
  "use_date_range": true,
  "date_range_mode": "today"
}
```

### Example 2: Yesterday

```json
{
  "use_date_range": true,
  "date_range_mode": "relative",
  "relative_days": -1
}
```

### Example 3: Custom Range

```json
{
  "use_date_range": true,
  "date_range_mode": "custom",
  "custom_start_date": "11/20/2025",
  "custom_end_date": "11/22/2025",
  "custom_start_time": "8:00 AM",
  "custom_end_time": "5:00 PM"
}
```

### Example 4: Disabled (Default)

```json
{
  "use_date_range": false
}
```

See [date_range_config_examples.md](file:///Users/nikicooke/.gemini/antigravity/brain/b6ae0824-a7a7-4ae9-bdef-e0ab7c519740/date_range_config_examples.md) for more examples.

## How It Works

1. Dashboard loads
2. If `use_date_range` is enabled:
   - Clicks "Customised" tab
   - Waits for date picker to appear
   - Fills in calculated dates and times
   - Clicks "Apply" button
   - Waits for filtered metrics response
3. Clicks "Refresh" button
4. Scrapes the filtered metrics

## Testing

To test this feature:

1. **Test with date range disabled** (default behavior):
   ```json
   {"use_date_range": false}
   ```

2. **Test with today's data**:
   ```json
   {"use_date_range": true, "date_range_mode": "today"}
   ```

3. **Test with custom range**:
   ```json
   {
     "use_date_range": true,
     "date_range_mode": "custom",
     "custom_start_date": "11/22/2025",
     "custom_end_date": "11/23/2025"
   }
   ```

4. Check logs for date selection steps and verify metrics match the selected range

## Notes

- Date format: `MM/DD/YYYY` (e.g., `11/23/2025`)
- Time format: 12-hour with AM/PM (e.g., `11:00 AM`, `2:30 PM`)
- If date picker is not available, scraper logs a warning and continues with default view
- All steps are logged for debugging

## Next Steps

- Merge to main after testing confirms functionality
- Update main README with date range configuration section
