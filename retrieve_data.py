"""Query OpenAlex for works using parameters defined in a YAML config file."""
from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import yaml
from pyalex import Works, config

DEFAULT_QUERY = "raman spectroscopy of plastics"
DEFAULT_PER_PAGE = 200
DEFAULT_MAX_PAGES = 10


@dataclass
class QueryFilters:
    year: int | None = None
    from_year: int | None = None
    to_year: int | None = None
    min_relevance: float | None = None
    extra_filters: dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryConfig:
    query: str = DEFAULT_QUERY
    per_page: int = DEFAULT_PER_PAGE
    max_pages: int = DEFAULT_MAX_PAGES
    search_name: str = "default_search"
    filters: QueryFilters = field(default_factory=QueryFilters)


def configure_mailto(mailto: str | None) -> None:
    """Configure the mailto parameter for polite OpenAlex usage."""
    if mailto:
        config["mailto"] = mailto
    else:
        config.pop("mailto", None)


def load_query_configs(path: str | os.PathLike[str]) -> list[QueryConfig]:
    """Load query options from a YAML configuration file. Supports multiple searches."""
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")

    with cfg_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    if not isinstance(raw, dict):
        raise ValueError("YAML config must define a mapping at the top level")

    # Check if 'searches' key exists for multiple searches
    if "searches" in raw:
        searches_list = raw["searches"]
        if not isinstance(searches_list, list):
            raise ValueError("'searches' must be a list in the YAML config")

        configs = []
        for idx, search_config in enumerate(searches_list):
            config = _parse_single_config(search_config, f"search_{idx}")
            configs.append(config)
        return configs
    else:
        # Single search configuration (backward compatible)
        return [_parse_single_config(raw, "default_search")]


def _parse_single_config(raw: dict, default_name: str) -> QueryConfig:
    """Parse a single search configuration from YAML."""
    query = raw.get("query", DEFAULT_QUERY)
    per_page = int(raw.get("per_page", DEFAULT_PER_PAGE))
    max_pages = int(raw.get("max_pages", DEFAULT_MAX_PAGES))
    search_name = raw.get("name", default_name)

    filters_section = raw.get("filters", {}) or {}
    if not isinstance(filters_section, dict):
        raise ValueError("'filters' must be a mapping in the YAML config")

    year = raw.get("year", filters_section.pop("year", None))
    from_year = raw.get("from_year", filters_section.pop("from_year", None))
    to_year = raw.get("to_year", filters_section.pop("to_year", None))
    min_relevance = raw.get("min_relevance", filters_section.pop("min_relevance", None))

    extra_filters: dict[str, Any] = {}
    for key, value in _flatten_filters(filters_section):
        normalized_key = _normalize_filter_key(key)
        if value is not None:
            extra_filters[normalized_key] = value

    return QueryConfig(
        query=query,
        per_page=per_page,
        max_pages=max_pages,
        search_name=search_name,
        filters=QueryFilters(
            year=_parse_optional_int(year),
            from_year=_parse_optional_int(from_year),
            to_year=_parse_optional_int(to_year),
            min_relevance=_parse_optional_float(min_relevance),
            extra_filters=extra_filters,
        ),
    )


def _flatten_filters(data: dict[str, Any], prefix: str = "") -> Iterable[tuple[str, Any]]:
    """Flatten nested YAML filter mappings into dotted keys for OpenAlex."""
    for raw_key, value in data.items():
        key = _normalize_filter_key(raw_key)
        new_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            yield from _flatten_filters(value, new_key)
        else:
            yield new_key, value


def _normalize_filter_key(key: str) -> str:
    if not isinstance(key, str):
        raise ValueError(f"Filter keys must be strings, got {key!r}")
    return key


def _parse_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Expected integer-compatible value, got {value!r}") from exc


def _parse_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Expected float-compatible value, got {value!r}") from exc


def build_query(cfg: QueryConfig) -> Works:
    """Build a Works query applying YAML-defined filters."""
    query = Works()
    if cfg.query:
        query = query.search(cfg.query)

    filters = cfg.filters
    if filters.year is not None:
        query = query.filter(publication_year=filters.year)
    else:
        if filters.from_year is not None:
            query = query.filter(from_publication_date=f"{filters.from_year}-01-01")
        if filters.to_year is not None:
            query = query.filter(to_publication_date=f"{filters.to_year}-12-31")

    if filters.extra_filters:
        query = query.filter(**filters.extra_filters)

    return query


def fetch_works(cfg: QueryConfig) -> tuple[int | None, list[dict]]:
    """Fetch works from OpenAlex using the official client with pagination support."""
    print(f"🎯 {cfg.search_name}...")
    print(f"   Query: {cfg.query}")

    base_query = build_query(cfg)

    try:
        total_count = base_query.count()
        print(f"   📊 Total available: {total_count:,}")
    except Exception as e:
        print(f"   ⚠️ Could not get total count: {e}")
        total_count = None

    all_results = []

    # Create paginator once - it returns a generator that yields pages
    # When using .search(), results are automatically sorted by relevance
    try:
        # Calculate total items to fetch
        max_items = cfg.max_pages * cfg.per_page
        pager = base_query.paginate(per_page=cfg.per_page, n_max=max_items)

        # Paginate through results
        for page_num in range(1, cfg.max_pages + 1):
            try:
                # Get next page of results
                page_results = next(pager)

                if not page_results:
                    print(f"   ✅ No more results at page {page_num}")
                    break

                # Apply relevance filter if specified
                filters = cfg.filters
                if filters.min_relevance is not None:
                    min_score = filters.min_relevance
                    page_results = [work for work in page_results if (work.get("relevance_score") or 0.0) >= min_score]

                all_results.extend(page_results)
                print(f"   📄 Page {page_num}: {len(page_results)} works (Total: {len(all_results)})")

                # Be polite to the API
                time.sleep(0.3)

            except StopIteration:
                print(f"   ✅ No more results at page {page_num}")
                break

    except Exception as e:
        print(f"   ❌ Error during pagination: {e}")

    return total_count, all_results


def format_authors(authorships: Iterable[dict]) -> str:
    names = []
    for author in authorships:
        author_obj = author.get("author", {})
        name = author_obj.get("display_name")
        if name:
            names.append(name)
    return ", ".join(names) if names else "Unknown authors"


def save_works_to_csv(works: list[dict], output_path: str | Path) -> None:
    """Save works to CSV file."""
    if not works:
        print("❌ No works to save")
        return

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Define fieldnames for CSV
    fieldnames = [
        "id",
        "title",
        "year",
        "doi",
        "journal",
        "citations",
        "oa_url",
        "oa_status",
        "publication_date",
        "authors",
        "relevance_score",
        "search_method"
    ]

    print(f"💾 Saving {len(works):,} works to {output_path}")

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()

        successful_rows = 0
        for work in works:
            try:
                # Extract paper data
                primary_location = work.get("primary_location", {}) or {}
                source = primary_location.get("source", {}) or {}
                journal = source.get("display_name", "Unknown")

                open_access = work.get("open_access", {}) or {}
                oa_url = open_access.get("oa_url", "")
                oa_status = open_access.get("oa_status", "unknown")

                row = {
                    "id": work.get("id", ""),
                    "title": work.get("display_name", "Untitled"),
                    "year": work.get("publication_year", ""),
                    "doi": work.get("doi", ""),
                    "journal": journal,
                    "citations": work.get("cited_by_count", 0),
                    "oa_url": oa_url,
                    "oa_status": oa_status,
                    "publication_date": work.get("publication_date", ""),
                    "authors": format_authors(work.get("authorships", [])),
                    "relevance_score": work.get("relevance_score", ""),
                    "search_method": work.get("search_method", "")
                }

                writer.writerow(row)
                successful_rows += 1
            except Exception as e:
                print(f"⚠️ Could not write row: {e}")
                continue

    print(f"✅ Successfully wrote {successful_rows} rows to {output_path}")


def render_results(results: Iterable[dict], *, total_count: int | None = None) -> None:
    results_list = list(results)
    if not results_list:
        print("No results returned.")
        return

    if total_count is not None:
        print(f"Found {total_count} matching works (showing {len(results_list)}):\n")

    for index, work in enumerate(results_list, start=1):
        title = work.get("display_name", "Untitled")
        publication_year = work.get("publication_year", "Unknown year")
        doi = work.get("doi")
        open_access_details = work.get("open_access") or {}
        host_venue = work.get("host_venue", {})
        journal = host_venue.get("display_name") or "Unknown venue"
        oa_status_value = open_access_details.get("oa_status")
        is_oa = open_access_details.get("is_oa")
        if oa_status_value:
            oa_status = f"OA status: {oa_status_value}"
        elif is_oa is True:
            oa_status = "Open Access"
        elif is_oa is False:
            oa_status = "Closed Access"
        else:
            oa_status = "Access status unknown"
        concepts = ", ".join(concept["display_name"] for concept in work.get("concepts", [])[:5])
        primary_location = work.get("primary_location", {})
        landing_page = primary_location.get("landing_page_url")

        print(f"Result {index}:")
        print(f"  Title: {title}")
        print(f"  Year: {publication_year}")
        print(f"  Journal: {journal} ({oa_status})")
        print(f"  Authors: {format_authors(work.get('authorships', []))}")
        oa_url = open_access_details.get("oa_url")
        if oa_url:
            print(f"  OA URL: {oa_url}")
        elif open_access_details.get("any_repository_has_fulltext"):
            print("  OA URL: repository fulltext available (check OpenAlex record)")
        cited_by = work.get("cited_by_count")
        if cited_by is not None:
            print(f"  Citations: {cited_by}")
        if doi:
            print(f"  DOI: {doi}")
        if landing_page:
            print(f"  URL: {landing_page}")
        if concepts:
            print(f"  Concepts: {concepts}")
        relevance = work.get("relevance_score")
        if relevance is not None:
            print(f"  Relevance score: {relevance:.3f}")
        print()


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Query OpenAlex using a YAML configuration file.")
    parser.add_argument("--config", required=True, help="Path to the YAML config file")
    parser.add_argument("--mailto", dest="mailto", default=os.environ.get("OPENALEX_MAILTO"), help="Contact email to include with the request")
    parser.add_argument("--output", dest="output", help="Path to save results as CSV (optional)")
    parser.add_argument("--no-render", dest="no_render", action="store_true", help="Skip rendering results to console")
    return parser


def validate_filters(filters: QueryFilters) -> None:
    if filters.year is not None and (filters.from_year is not None or filters.to_year is not None):
        raise ValueError("'year' cannot be combined with 'from_year' or 'to_year'")
    if filters.from_year is not None and filters.to_year is not None and filters.from_year > filters.to_year:
        raise ValueError("'from_year' must be less than or equal to 'to_year'")


def main(argv: list[str] | None = None) -> int:
    parser = create_parser()
    args = parser.parse_args(argv)

    try:
        # Load configurations (can be multiple searches)
        configs = load_query_configs(args.config)

        # Validate filters for each config
        for cfg in configs:
            validate_filters(cfg.filters)

        configure_mailto(args.mailto)

        print("=" * 70)
        print("🔬 OPENALEX WORKS COLLECTION")
        print("=" * 70)
        print(f"Number of searches: {len(configs)}")
        print()

        all_works = []
        seen_ids = set()

        # Fetch works for each search configuration
        for cfg in configs:
            total_count, results = fetch_works(cfg)

            # Tag results with search method
            for work in results:
                work["search_method"] = cfg.search_name

            # Deduplicate based on work ID
            new_works = 0
            for work in results:
                work_id = work.get("id")
                if work_id and work_id not in seen_ids:
                    seen_ids.add(work_id)
                    all_works.append(work)
                    new_works += 1

            print(f"   ✅ Added {new_works} new works from {cfg.search_name}")
            print(f"   📈 Total unique works so far: {len(all_works):,}\n")

            # Be polite to the API between searches
            time.sleep(1)

        print(f"\n📊 COLLECTION COMPLETE!")
        print(f"   Total works collected: {len(all_works):,}")

        # Save to CSV if output path is provided
        if args.output:
            save_works_to_csv(all_works, args.output)

        # Render results to console unless --no-render is specified
        if not args.no_render:
            render_results(all_works, total_count=len(all_works))

    except Exception as exc:  # pragma: no cover - network layer / config errors
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
