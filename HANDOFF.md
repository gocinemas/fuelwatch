# Handoff: Factory, Books, Intel, Miru (as of 23 Nov 2026)

Start a new session here: /Users/srevi/fuelwatch (Miru) and /Users/srevi/factory (Factory pages).
Do NOT print secret values. Keys live in Railway (project fuelwatch, web service) and in Supabase/Algolia dashboards.

## Repos and deploys
- Miru: /Users/srevi/fuelwatch, pushed to main -> Railway auto-deploys. Check: `railway deployment list`.
- Factory pages (humanagency.co/factory, /intel, /books): /Users/srevi/factory, pushed to main -> Hostinger (takes a few minutes).
- Intel repo: humanagencyco/intel (copy at /Users/srevi/intel-front). Factory copy is at /Users/srevi/factory/intel/.
- Always commit only the intended hunks in sms_service.py (working copy has an unrelated personal_intel block).
- Run ./dev/check.sh in /Users/srevi/factory before pushing.

## Books (Factory /books)
- Data: Supabase project "Books" (URL wagbceigsvctbdthfczf). Tables: book_catalogue (4,982+ books), book_readers, book_list, book_link_codes. SQL files in /Users/srevi/fuelwatch/*.sql (all run).
- Server: /Users/srevi/fuelwatch/books_api.py (readers, lists, due dates, link codes, slip reader, rename, theme/avatar).
- Search: Algolia index "books" (synced from Supabase by /private/tmp/.../scratchpad/sync_algolia.py). Search key is public (in page). Admin key is ALGOLIA_BOOKS_KEY in Railway (was printed in chat earlier: ROTATE).
- Done: name screen, rename, start again, theme+avatar picker, library shelf with due dates, read-a-library-slip (photo -> titles), barcode photo scan, author page, "Picked for you" suggestions per favourite, reading goal + badges (goal kept on device), link code for second device.
- Pending: Inaaya's ~100-book list (she is the reader; merge into her list via API); tests on her iPad (link code, barcode, slip photo); favourites (heart + favourite author); badge progress "2 of 5"; Waterstones link + Ticketmaster kids' shows (Eventbrite search is 404, do not use).
- Shelf names now: Reading, Read, From the library, Someday.

## Factory (humanagency.co/factory)
- Bin day: lives in Personal section only. Any postcode -> council (postcodes.io). Runnymede (E07000212) gives addresses + real dates via its own form. Other councils: link to council site (353/361 websites in councils_phase1.csv).
- WhenIsBins integration built in bins_lookup.py but OFF (env BINS_WHENISBINS_ENABLED must stay unset until they agree to commercial use). Draft email to hello@whenisbins.com exists in Gmail; user to confirm if sent.
- Council checkers: Hull, Lewisham, Adur tested: no quick path. Jadu 5/53 found, Confirm not finished.

## Intel (humanagency.co/factory/intel)
- Search (Miru /api/companies/search), company card (/api/company/<slug>), hiring (/api/hiring-signals), brand products + news (/api/brand/intelligence), AI use + SWOT (/api/company/<slug>/analysis, Claude).
- Pending: barcode scan for products (reuse /api/scan-barcode + Open Food Facts), Companies House facts, more companies researched.
- Old intel.humanagency.co still shows old design (not fixed).

## Receipts / expenses (Miru)
- Receipts go in via WhatsApp to Miru (wa_saves: summary has DATE + total, category). Spend card totals by month.
- Next task: web upload on Factory Personal section (drop batch of photos, review date/amount/category, save via Miru API into wa_saves path). User has October and November receipt photos.

## Security to do
- Rotate: Algolia admin key, Anthropic key, DB password, Fuel Finder secret, Feedbin password (all printed in chat earlier).
- Consider sign-in for families (books lists currently tied to the device ID).

## Open decisions for the user
- Vouchers/incentives: later.
- Whether to make the code repos private.
