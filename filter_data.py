import csv
import os
from collections import Counter
import glob
import argparse

def deduplicate_papers(papers):
    """
    Deduplicate papers based on title (case-insensitive).
    Returns deduplicated list and count of duplicates removed.
    """
    seen_titles = {}
    deduplicated = []
    duplicates_removed = 0

    for paper in papers:
        title = paper.get('title', '').strip().lower()

        # Skip if title is empty
        if not title:
            deduplicated.append(paper)
            continue

        # Check if we've seen this title before
        if title not in seen_titles:
            seen_titles[title] = True
            deduplicated.append(paper)
        else:
            duplicates_removed += 1

    return deduplicated, duplicates_removed

def filter_by_oa_status(papers, oa_status):
    """
    Filter papers by open access status.
    Returns filtered list and count of papers filtered out.
    """
    filtered = []
    filtered_out = 0

    oa_status_lower = oa_status.lower()

    for paper in papers:
        paper_oa_status = paper.get('oa_status', '').strip().lower()

        if paper_oa_status == oa_status_lower:
            filtered.append(paper)
        else:
            filtered_out += 1

    return filtered, filtered_out

def count_papers_by_year(csv_filepath, deduplicate=False, oa_status=None):
    """Count papers by publication year, with optional deduplication and OA filtering"""

    if not os.path.exists(csv_filepath):
        print(f"❌ File not found: {csv_filepath}")
        return None

    year_counts = Counter()
    total_papers = 0
    papers_without_year = 0
    duplicates_removed = 0
    oa_filtered_out = 0

    print(f"\n📖 Reading papers from CSV...")

    with open(csv_filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        papers = list(reader)
        total_papers = len(papers)

    # Deduplicate if requested
    if deduplicate:
        print(f"🔍 Deduplicating papers based on title...")
        papers, duplicates_removed = deduplicate_papers(papers)
        print(f"   Removed {duplicates_removed:,} duplicate papers")
        print(f"   Remaining papers: {len(papers):,}")

    # Filter by OA status if requested
    if oa_status:
        print(f"🔍 Filtering papers by OA status: {oa_status}...")
        papers, oa_filtered_out = filter_by_oa_status(papers, oa_status)
        print(f"   Filtered out {oa_filtered_out:,} papers")
        print(f"   Remaining papers: {len(papers):,}")

    # Count papers by year
    for row in papers:
        year = row.get('year', '').strip()

        # Check if year is valid
        if year and year.isdigit():
            year_counts[int(year)] += 1
        else:
            papers_without_year += 1

    return year_counts, total_papers, papers_without_year, duplicates_removed, oa_filtered_out, papers, fieldnames

def display_results(year_counts, total_papers, papers_without_year, duplicates_removed=0, oa_filtered_out=0):
    """Display the results in a nice format"""

    print("\n" + "=" * 70)
    print("📊 PAPERS COUNT BY YEAR")
    print("=" * 70)

    print(f"\nTotal papers (original): {total_papers:,}")
    if duplicates_removed > 0:
        print(f"Duplicates removed: {duplicates_removed:,}")
        print(f"Unique papers: {total_papers - duplicates_removed:,}")
    if oa_filtered_out > 0:
        print(f"Filtered by OA status: {oa_filtered_out:,}")
        print(f"Papers after filtering: {total_papers - duplicates_removed - oa_filtered_out:,}")
    print(f"Papers with year: {sum(year_counts.values()):,}")
    print(f"Papers without year: {papers_without_year:,}")

    if not year_counts:
        print("\n❌ No papers with valid years found")
        return

    # Sort by year
    sorted_years = sorted(year_counts.items())

    print("\n" + "-" * 70)
    print(f"{'Year':<10} {'Count':<10} {'Bar Chart'}")
    print("-" * 70)

    # Find max count for scaling the bar chart
    max_count = max(year_counts.values())

    for year, count in sorted_years:
        # Create a simple bar chart
        bar_length = int((count / max_count) * 50)
        bar = "█" * bar_length

        print(f"{year:<10} {count:<10,} {bar}")

    print("-" * 70)

    # Show some statistics
    years = [y for y, _ in sorted_years]
    print(f"\n📅 Year range: {min(years)} - {max(years)}")
    print(f"📈 Average papers per year: {sum(year_counts.values()) / len(year_counts):.1f}")
    print(f"🔥 Peak year: {max(year_counts, key=year_counts.get)} with {year_counts[max(year_counts, key=year_counts.get)]:,} papers")

def save_year_counts_to_csv(year_counts, output_filepath):
    """Save the year counts to a new CSV file"""

    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_filepath)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(output_filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Year', 'Paper Count'])

        # Sort by year
        for year, count in sorted(year_counts.items()):
            writer.writerow([year, count])

    print(f"\n💾 Saved year counts to: {output_filepath}")

def save_filtered_papers_to_csv(papers, fieldnames, output_filepath):
    """Save the filtered/deduplicated papers to a new CSV file"""

    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_filepath)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(output_filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(papers)

    print(f"\n💾 Saved {len(papers):,} filtered papers to: {output_filepath}")

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Count papers by year from OpenAlex results and optionally filter duplicates',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Count papers by year (output: year counts CSV)
  python filter_data.py -i input.csv -o year_counts.csv

  # Deduplicate and save filtered papers (output: deduplicated papers CSV)
  python filter_data.py -i input.csv -o filtered_papers.csv --deduplicate

  # Deduplicate and filter by OA status
  python filter_data.py -i input.csv -o filtered_papers.csv -d --oa gold

  # Short form with OA filtering
  python filter_data.py -i raman_papers_final/raman_results.csv -o results/filtered.csv -d --oa green

Note:
  - Without --deduplicate: Output CSV contains year counts
  - With --deduplicate: Output CSV contains filtered papers with all original fields
  - --oa flag requires --deduplicate flag to be present
  - Valid OA status values: green, gold, hybrid, bronze
        """
    )
    parser.add_argument(
        '-i', '--input',
        required=True,
        help='Path to input CSV file containing papers'
    )
    parser.add_argument(
        '-o', '--output',
        required=True,
        help='Path to output CSV file (year counts without -d, filtered papers with -d)'
    )
    parser.add_argument(
        '-d', '--deduplicate',
        action='store_true',
        help='Deduplicate papers based on title (case-insensitive) and save filtered papers'
    )
    parser.add_argument(
        '--oa',
        choices=['green', 'gold', 'hybrid', 'bronze'],
        help='Filter by open access status (requires --deduplicate flag). Valid values: green, gold, hybrid, bronze'
    )

    args = parser.parse_args()

    # Validate that --oa requires --deduplicate
    if args.oa and not args.deduplicate:
        parser.error("--oa flag requires --deduplicate flag to be present")

    print("🔬 RAMAN PAPERS - ANALYSIS & FILTERING")
    print("=" * 70)

    if args.deduplicate:
        print("🔄 Deduplication: ENABLED (output will contain filtered papers)")
    else:
        print("🔄 Deduplication: DISABLED (output will contain year counts)")

    if args.oa:
        print(f"🔍 OA Status Filter: {args.oa.upper()}")

    print(f"📥 Input file: {args.input}")
    print(f"📤 Output file: {args.output}")

    # Check if input file exists
    if not os.path.exists(args.input):
        print(f"\n❌ Error: Input file not found: {args.input}")
        return

    # Count papers by year
    result = count_papers_by_year(args.input, deduplicate=args.deduplicate, oa_status=args.oa)

    if result is None:
        return

    year_counts, total_papers, papers_without_year, duplicates_removed, oa_filtered_out, papers, fieldnames = result

    # Display results
    display_results(year_counts, total_papers, papers_without_year, duplicates_removed, oa_filtered_out)

    # Save to CSV
    if args.deduplicate:
        # When deduplication is enabled, save the filtered papers
        save_filtered_papers_to_csv(papers, fieldnames, args.output)
    else:
        # Without deduplication, save year counts
        if year_counts:
            save_year_counts_to_csv(year_counts, args.output)

    print("\n✅ Analysis complete!")

if __name__ == "__main__":
    main()
