# Changelog

## 2.14.0
  * Updates to run on python 3.11.7 [#249](https://github.com/singer-io/tap-hubspot/pull/249mag)

## 2.13.2
  * Fix out-of-index error [#253](https://github.com/singer-io/tap-hubspot/pull/253)

## 2.13.1
  * Optimise contacts_by_company implementation [#250](https://github.com/singer-io/tap-hubspot/pull/250)

## 2.13.0
  * HubSpot Custom CRM Objects Support [#242](https://github.com/singer-io/tap-hubspot/pull/242)

## 2.12.2
  * Use engagements_page_size advanced option [#234](https://github.com/singer-io/tap-hubspot/pull/234)
  * 
## 2.12.1
  * Use sync start time for writing bookmarks [#226](https://github.com/singer-io/tap-hubspot/pull/226)

## 2.12.0
  * Include properties(default + custom) in tickets stream [#220](https://github.com/singer-io/tap-hubspot/pull/220)

## 2.11.0
  * Implement new stream - `tickets` [#218](https://github.com/singer-io/tap-hubspot/pull/218)
  * Update integration tests for the tickets stream implementation [#219](https://github.com/singer-io/tap-hubspot/pull/219)

## 2.10.0
  * Updated replication method as INCREMENTAL and replication key as property_hs_lastmodifieddate for deals and companies streams [#195](https://github.com/singer-io/tap-hubspot/pull/195)
  * Fixed Pylint errors [#204](https://github.com/singer-io/tap-hubspot/pull/204)

## 2.9.6
  * Implement Request Timeout [#177](https://github.com/singer-io/tap-hubspot/pull/177)
  * Add version timestamp in contacts [#191](https://github.com/singer-io/tap-hubspot/pull/191

## 2.9.5
  * Fixes a bug in sending the fields to the v3 Deals endpoint [#145](https://github.com/singer-io/tap-hubspot/pull/145)

## 2.9.4
  * Reverts 142 [#144](https://github.com/singer-io/tap-hubspot/pull/144)

## 2.9.3
  * Add support for property_versions [#142](https://github.com/singer-io/tap-hubspot/pull/142)

## 2.9.2
  * Change `POST` to V3 Deals to use one non-standard field instead of all fields we want [#139](https://github.com/singer-io/tap-hubspot/pull/139)
    * See the pull request for a more detailed explaination

## 2.9.1
  * Add retry logic to V3 calls [#136](https://github.com/singer-io/tap-hubspot/pull/136)

## 2.9.0
  * Add fields to Deals stream - `hs_date_entered_*` and `hs_date_exited_*` [#133](https://github.com/singer-io/tap-hubspot/pull/133)

## 2.8.1
  * Reverts `v2.8.0` back to `v.2.7.0`

## 2.8.0
  * Add fields to Deals stream - `hs_date_entered_*` and `hs_date_exited_*` [#124](https://github.com/singer-io/tap-hubspot/pull/124)

## 2.7.0
  * Fields nested under `properties` are copied to top level and prepended with `property_` [#107](https://github.com/singer-io/tap-hubspot/pull/107)

## 2.6.5
  * For `deals` stream, use `includeAllProperties` flag instead of appending all properties to request url [#112](https://github.com/singer-io/tap-hubspot/pull/112)

## 2.6.4
  * When making `deals` requests, only attach `properties` if selected [#102](https://github.com/singer-io/tap-hubspot/pull/102)

## 2.6.3
  * Use the metadata library better

## 2.6.2
  * Revert the revert. Go back to v2.6.0.

## 2.6.1
  * Revert v2.6.0 to v.2.5.2

## 2.6.0
  * Replaced `annotated_schema` with Singer `metadata`
  * Added integration tests to CircleCI

## 2.5.2
  * Companies and Engagements have a new pattern to catch records that are updated during a long-running sync. Rather than using a lookback window, the bookmark value will be limited to the `min(current_sync_start, max_bk_seen)` [#98](https://github.com/singer-io/tap-hubspot/pull/98)

## 2.4.0
  * The owners stream can optionally fetch "inactive owners" [#92](https://github.com/singer-io/tap-hubspot/pull/92)

## 2.3.0
  * Engagements will now track how long the stream takes to sync, and look back on the next run by that amount to cover potentially missed updates due to asynchronous updates during the previous sync [#91](https://github.com/singer-io/tap-hubspot/pull/91)

## 2.2.8
  * When resuming an interrupted sync, will now attempt all streams before exiting [#90](https://github.com/singer-io/tap-hubspot/pull/90)

## 2.2.7
  * Add `delivered`, `forward`, `print`, `reply`, `spamreport` to `campaigns.counters`

## 2.2.6
  * Change a loop over `dict.items()` to `dict.values()` because the keys returned were not being used [#82](https://github.com/singer-io/tap-hubspot/pull/82)

## 2.2.5
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 2.2.4
  * Ensure that deal associations are being retrieved if `associations` are selected in the catalog [#79](https://github.com/singer-io/tap-hubspot/pull/79)

## 2.2.3
  * Scrub the access token from error messages Hubspot returns when there are insufficient permissions [#75](https://github.com/singer-io/tap-hubspot/pull/75)

## 2.2.2
  * Fix a bug with the 'engagements' stream which requires the 'engagement' field to have automatic inclusion [#74](https://github.com/singer-io/tap-hubspot/pull/74)

## 2.2.1
  * Fix a bug with the 'inclusion' metadata for replication_key fields [#72](https://github.com/singer-io/tap-hubspot/pull/72)

## 2.2.0
  * Adds property selection to the tap [#67](https://github.com/singer-io/tap-hubspot/pull/67)
  * Removed the keywords stream as it is deprecated [#68](https://github.com/singer-io/tap-hubspot/pull/68)
  * Schema updates [#69](https://github.com/singer-io/tap-hubspot/pull/69) [#70](https://github.com/singer-io/tap-hubspot/pull/70)
