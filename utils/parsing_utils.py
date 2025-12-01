"""
Parsing utilities for extracting structured data from OCR text
"""

import re
from typing import Dict, Any, List


def _parse_dynamic_headers(header_line: str) -> List[str]:
    """
    Dynamically parse complex header line with multi-word headers and units.
    Handles patterns like "Sample ID", "N (%)", "Org. C (%)", "Total P (mg/kg)", etc.
    """
    # Use regex to find patterns: word(s) followed by optional unit in parentheses
    # This handles: "Sample ID", "pH", "N (%)", "Org. C (%)", "Total P (mg/kg)", etc.
    pattern = r'([A-Za-z][A-Za-z\s.]*(?:\s*\([^)]+\))?)'
    matches = re.findall(pattern, header_line)

    headers = []
    for match in matches:
        clean_header = match.strip()
        if clean_header and len(clean_header) > 1:  # Skip single characters
            headers.append(clean_header)

    # Post-process to combine related parts that might have been split incorrectly
    processed_headers = []
    i = 0
    while i < len(headers):
        current = headers[i]

        # Handle "Sample ID" pattern
        if current.lower() == "sample" and i + 1 < len(headers) and headers[i + 1].lower() == "id":
            processed_headers.append("Sample ID")
            i += 2
        # Handle "Org. C" pattern
        elif current.lower() == "org." and i + 1 < len(headers) and headers[i + 1].lower().startswith("c"):
            unit_part = headers[i + 1] if i + 2 >= len(headers) else f"{headers[i + 1]} {headers[i + 2]}" if headers[i + 2].startswith("(") else headers[i + 1]
            processed_headers.append(f"Org. C {unit_part}" if "(" in unit_part else f"Org. C ({unit_part})")
            i += 2 if not unit_part.startswith("(") or i + 2 >= len(headers) else 3
        # Handle "Total P", "Avail P" patterns
        elif current.lower() in ["total", "avail"] and i + 1 < len(headers) and headers[i + 1].lower() == "p":
            unit_part = headers[i + 2] if i + 2 < len(headers) and headers[i + 2].startswith("(") else "(mg/kg)"
            processed_headers.append(f"{current} P {unit_part}")
            i += 3 if i + 2 < len(headers) and headers[i + 2].startswith("(") else 2
        # Handle "Exch. K", "Exch. Ca", "Exch. Mg" patterns
        elif current.lower() == "exch." and i + 2 < len(headers):
            element = headers[i + 1]
            unit = headers[i + 2] if headers[i + 2].startswith("(") else "(meq/100 g)"
            processed_headers.append(f"Exch. {element} {unit}")
            i += 3 if headers[i + 2].startswith("(") else 2
        else:
            processed_headers.append(current)
            i += 1

    return processed_headers


def _parse_raw_text_to_structured_json(raw_text: str) -> dict:
    """
    Parse raw OCR text into structured JSON.
    Simplified and robust parsing for soil/leaf analysis data.
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"Parsing raw text: {len(raw_text)} characters")
    logger.debug(f"Raw text content: {raw_text[:300]}...")

    # Also print for immediate debugging
    print(f"DEBUG: Parsing raw text: {len(raw_text)} characters")
    print(f"DEBUG: Raw text preview: {raw_text[:200]}...")

    # Quick check for obvious indicators
    if "soil" in raw_text.lower():
        print("DEBUG: Found 'soil' in raw text")
    if "ph" in raw_text.lower():
        print("DEBUG: Found 'ph' in raw text")
    if "cec" in raw_text.lower():
        print("DEBUG: Found 'cec' in raw text")

    # Step 1: Clean and split the text
    lines = [line.strip() for line in raw_text.split('\n') if line.strip()]

    logger.info(f"Found {len(lines)} non-empty lines after splitting by newlines")

    # Handle single-line OCR output (common with some OCR engines)
    if len(lines) == 1 and len(raw_text) > 100:
        logger.info("Detected single-line OCR output, attempting to parse as single line")
        single_line = lines[0]

        # Try different splitting strategies for single-line content
        # Strategy 1: Split by multiple spaces (tabular data)
        if '  ' in single_line:  # Multiple spaces indicate tabular format
            potential_lines = []
            parts = single_line.split()

            # Look for sample IDs to determine line breaks
            current_line_parts = []
            for part in parts:
                current_line_parts.append(part)

                # If we find a sample ID (S001, L001, etc.), start a new line after collecting enough data
                if re.match(r'^[SL]\d{3,4}$', part) and len(current_line_parts) >= 2:
                    # Check if we have enough parts for a complete sample line
                    remaining_parts = parts[len(current_line_parts):]
                    if len(remaining_parts) >= 8:  # At least sample ID + 8 parameters
                        potential_lines.append(' '.join(current_line_parts))
                        current_line_parts = []

            # Add remaining parts if any
            if current_line_parts:
                potential_lines.append(' '.join(current_line_parts))

            if len(potential_lines) >= 2:  # Need at least header + 1 data line
                lines = potential_lines
                logger.info(f"Successfully split single line into {len(lines)} lines using sample ID detection")
            else:
                # Strategy 2: Try to identify header vs data sections
                words = single_line.split()
                header_end_idx = -1

                # Look for common header patterns
                for i, word in enumerate(words):
                    if word.lower() in ['s001', 'l001'] or (word.isdigit() and len(word) == 3 and word.startswith(('4', '5', '6', '7', '2'))):
                        header_end_idx = i
                        break

                if header_end_idx > 0:
                    header_line = ' '.join(words[:header_end_idx])
                    data_section = ' '.join(words[header_end_idx:])
                    lines = [header_line, data_section]
                    logger.info("Split single line into header and data sections")
                else:
                    # Strategy 3: Assume first part is title/header, rest is data
                    words = single_line.split()
                    if len(words) > 10:  # Arbitrary threshold for meaningful content
                        lines = [single_line]  # Keep as single line but proceed with parsing
                        logger.info("Keeping as single line, will attempt direct parsing")
                    else:
                        logger.warning("Single line too short for meaningful parsing")
                        return {"type": "unknown", "samples": []}
        else:
            logger.info("Single line detected but no clear tabular structure, proceeding with direct parsing")

    logger.info(f"Final line count after processing: {len(lines)}")
    for i, line in enumerate(lines[:5]):  # Log first 5 lines
        logger.debug(f"Line {i+1}: {repr(line)}")

    if len(lines) < 1:  # Need at least one line
        logger.warning(f"No lines found for parsing")
        return {"type": "unknown", "samples": []}

    # If we have very few lines, still try to parse them
    if len(lines) < 3:
        logger.info(f"Only {len(lines)} lines available, attempting flexible parsing")

    # Step 2: Detect if this is soil or leaf data - try multiple approaches
    logger.info("Attempting to detect data type...")

    # First, try with the second line (assuming it's the header)
    header_line = lines[1].lower() if len(lines) > 1 else ""
    logger.info(f"Header line (line 2): {repr(header_line)}")

    container_type = None
    expected_headers = []

    # Check if it's soil data (prioritize specific soil indicators)
    soil_keywords = ["cec", "exch", "exchangeable", "organic", "org"]
    soil_specific = ["cec (meq%)", "exch. k", "exch. ca", "exch. mg", "avail p"]
    leaf_keywords = ["b (mg/kg)", "cu (mg/kg)", "zn (mg/kg)", "fe (mg/kg)", "mn (mg/kg)"]
    leaf_specific = ["b (mg/kg)", "cu (mg/kg)", "zn (mg/kg)"]

    # Count specific indicators to determine type
    soil_score = sum(1 for keyword in soil_specific if keyword in header_line)
    soil_score += sum(1 for keyword in soil_keywords if keyword in header_line)

    leaf_score = sum(1 for keyword in leaf_specific if keyword in header_line)
    leaf_score += sum(1 for keyword in leaf_keywords if keyword in header_line)

    # Also check for general nutrient indicators
    general_keywords = ["n (%)", "p (%)", "k (%)", "ph", "nitrogen", "phosphorus", "potassium"]
    if any(keyword in header_line for keyword in general_keywords):
        if soil_score >= leaf_score:
            soil_score += 1
        else:
            leaf_score += 1

    logger.info(f"Detection scores - Soil: {soil_score}, Leaf: {leaf_score}")
    print(f"DEBUG: Detection scores - Soil: {soil_score}, Leaf: {leaf_score}")

    if soil_score > leaf_score:
        container_type = "soil"
        expected_headers = [
            "Sample ID", "pH", "N (%)", "Org. C (%)", "Total P (mg/kg)",
            "Avail P (mg/kg)", "Exch. K (meq%)", "Exch. Ca (meq%)",
            "Exch. Mg (meq%)", "CEC (meq%)"
        ]
        logger.info("✅ Detected soil analysis data from line 2")
    elif leaf_score > soil_score:
        container_type = "leaf"
        expected_headers = [
            "Sample ID", "N (%)", "P (%)", "K (%)", "Mg (%)", "Ca (%)",
            "B (mg/kg)", "Cu (mg/kg)", "Zn (mg/kg)", "Fe (mg/kg)", "Mn (mg/kg)"
        ]
        logger.info("✅ Detected leaf analysis data from line 2")

    # If still not detected, try other lines
    if not container_type:
        logger.info("Trying other lines for data type detection...")
        for i, line in enumerate(lines):
            line_lower = line.lower()
            logger.debug(f"Checking line {i+1}: {repr(line_lower)}")

            # Use same scoring system for other lines
            line_soil_score = sum(1 for keyword in soil_specific if keyword in line_lower)
            line_soil_score += sum(1 for keyword in soil_keywords if keyword in line_lower)

            line_leaf_score = sum(1 for keyword in leaf_specific if keyword in line_lower)
            line_leaf_score += sum(1 for keyword in leaf_keywords if keyword in line_lower)

            if any(keyword in line_lower for keyword in general_keywords):
                if line_soil_score >= line_leaf_score:
                    line_soil_score += 1
                else:
                    line_leaf_score += 1

            if line_soil_score > line_leaf_score:
                container_type = "soil"
                expected_headers = [
                    "Sample ID", "pH", "N (%)", "Org. C (%)", "Total P (mg/kg)",
                    "Avail P (mg/kg)", "Exch. K (meq%)", "Exch. Ca (meq%)",
                    "Exch. Mg (meq%)", "CEC (meq%)"
                ]
                logger.info(f"✅ Detected soil analysis data from line {i+1} (scores: soil={line_soil_score}, leaf={line_leaf_score})")
                break
            elif line_leaf_score > line_soil_score:
                container_type = "leaf"
                expected_headers = [
                    "Sample ID", "N (%)", "P (%)", "K (%)", "Mg (%)", "Ca (%)",
                    "B (mg/kg)", "Cu (mg/kg)", "Zn (mg/kg)", "Fe (mg/kg)", "Mn (mg/kg)"
                ]
                logger.info(f"✅ Detected leaf analysis data from line {i+1} (scores: soil={line_soil_score}, leaf={line_leaf_score})")
                break

    # If still not detected, try to infer from sample patterns
    if not container_type:
        logger.info("Trying to infer data type from sample patterns...")
        sample_patterns = ["s001", "s002", "l001", "l002", "sample"]
        for line in lines:
            line_lower = line.lower()
            logger.debug(f"Checking line for detection: {repr(line_lower)}")

            # For single-line content, also check for specific keywords
            all_soil_keywords = ["cec", "exch", "exchangeable", "organic", "org", "ph", "phosphorus", "potassium", "calcium", "magnesium"]
            all_leaf_keywords = ["b (mg/kg)", "cu (mg/kg)", "zn (mg/kg)", "fe (mg/kg)", "mn (mg/kg)", "nitrogen", "phosphorus", "potassium"]

            # Check if we have sample patterns or keywords
            has_sample_pattern = any(pattern in line_lower for pattern in sample_patterns)
            has_soil_keywords = any(keyword in line_lower for keyword in all_soil_keywords)
            has_leaf_keywords = any(keyword in line_lower for keyword in all_leaf_keywords)

            logger.debug(f"Line analysis - Sample pattern: {has_sample_pattern}, Soil keywords: {has_soil_keywords}, Leaf keywords: {has_leaf_keywords}")

            if has_sample_pattern or has_soil_keywords or has_leaf_keywords:
                # Count keywords to determine type
                soil_count = sum(1 for keyword in all_soil_keywords if keyword in line_lower)
                leaf_count = sum(1 for keyword in all_leaf_keywords if keyword in line_lower)

                # Check for specific soil indicators that are more definitive
                soil_specific = ["cec", "exch", "exchangeable", "avail p"]
                leaf_specific = ["b (mg/kg)", "cu (mg/kg)", "zn (mg/kg)"]

                soil_specific_count = sum(1 for keyword in soil_specific if keyword in line_lower)
                leaf_specific_count = sum(1 for keyword in leaf_specific if keyword in line_lower)

                soil_count += soil_specific_count * 2  # Weight specific indicators more
                leaf_count += leaf_specific_count * 2

                logger.info(f"Keyword counts - Soil: {soil_count}, Leaf: {leaf_count}")

                if soil_count > leaf_count:
                    container_type = "soil"
                    expected_headers = [
                        "Sample ID", "pH", "N (%)", "Org. C (%)", "Total P (mg/kg)",
                        "Avail P (mg/kg)", "Exch. K (meq%)", "Exch. Ca (meq%)",
                        "Exch. Mg (meq%)", "CEC (meq%)"
                    ]
                    logger.info("✅ Inferred soil analysis data from sample patterns")
                elif leaf_count > soil_count:
                    container_type = "leaf"
                    expected_headers = [
                        "Sample ID", "N (%)", "P (%)", "K (%)", "Mg (%)", "Ca (%)",
                        "B (mg/kg)", "Cu (mg/kg)", "Zn (mg/kg)", "Fe (mg/kg)", "Mn (mg/kg)"
                    ]
                    logger.info("✅ Inferred leaf analysis data from sample patterns")
                elif soil_count > 0:  # If we have soil keywords but tie with leaf, prefer soil
                    container_type = "soil"
                    expected_headers = [
                        "Sample ID", "pH", "N (%)", "Org. C (%)", "Total P (mg/kg)",
                        "Avail P (mg/kg)", "Exch. K (meq%)", "Exch. Ca (meq%)",
                        "Exch. Mg (meq%)", "CEC (meq%)"
                    ]
                    logger.info("✅ Defaulted to soil analysis data (soil keywords present)")
                else:
                    # Last resort - assume soil if we can't determine
                    container_type = "soil"
                    expected_headers = [
                        "Sample ID", "pH", "N (%)", "Org. C (%)", "Total P (mg/kg)",
                        "Avail P (mg/kg)", "Exch. K (meq%)", "Exch. Ca (meq%)",
                        "Exch. Mg (meq%)", "CEC (meq%)"
                    ]
                    logger.info("✅ Defaulted to soil analysis data (fallback)")
                break

    # *** FINAL FALLBACK: If still not detected, check the entire text ***
    if not container_type:
        logger.warning("All detection methods failed, checking entire raw text...")
        print("DEBUG: ENTERING FINAL FALLBACK")
        raw_text_lower = raw_text.lower()
        print(f"DEBUG: Raw text lower: {raw_text_lower[:200]}...")

        # Very simple fallback detection
        soil_check = "soil" in raw_text_lower or "ph" in raw_text_lower or "cec" in raw_text_lower
        leaf_check = "leaf" in raw_text_lower or any(kw in raw_text_lower for kw in ["b (mg/kg)", "cu (mg/kg)", "zn (mg/kg)"])

        print(f"DEBUG: Soil check: {soil_check}, Leaf check: {leaf_check}")

        if soil_check:
            container_type = "soil"
            expected_headers = [
                "Sample ID", "pH", "N (%)", "Org. C (%)", "Total P (mg/kg)",
                "Avail P (mg/kg)", "Exch. K (meq%)", "Exch. Ca (meq%)",
                "Exch. Mg (meq%)", "CEC (meq%)"
            ]
            logger.info("✅ FINAL FALLBACK: Detected soil data from raw text")
            print("DEBUG: FINAL FALLBACK - Detected SOIL")
        elif leaf_check:
            container_type = "leaf"
            expected_headers = [
                "Sample ID", "N (%)", "P (%)", "K (%)", "Mg (%)", "Ca (%)",
                "B (mg/kg)", "Cu (mg/kg)", "Zn (mg/kg)", "Fe (mg/kg)", "Mn (mg/kg)"
            ]
            logger.info("✅ FINAL FALLBACK: Detected leaf data from raw text")
            print("DEBUG: FINAL FALLBACK - Detected LEAF")
        else:
            logger.warning("❌ FINAL FALLBACK: Could not detect data type from raw text")
            logger.warning(f"Raw text sample: {raw_text[:200]}...")
            print("DEBUG: FINAL FALLBACK - FAILED")

    if not container_type:
        logger.warning("❌ Could not detect data type from any line")
        logger.warning("Available lines:")
        for i, line in enumerate(lines[:10]):  # Show first 10 lines
            logger.warning(f"  Line {i+1}: {repr(line)}")
        return {"type": "unknown", "samples": []}

    # Step 3: Parse data rows - flexible parsing for various formats
    samples = []
    logger.info(f"Starting sample parsing with {len(expected_headers)} expected headers")

    # Handle single-line parsing differently
    if len(lines) == 1:
        logger.info("Parsing single-line content")
        single_line = lines[0]
        parts = single_line.split()

        # For single line, we need to find all sample IDs and their corresponding values
        sample_data_sections = []
        current_section = []

        for part in parts:
            if re.match(r'^[SL]\d{3,4}$', part.strip()):  # Found a sample ID
                if current_section:  # Save previous section if it exists
                    sample_data_sections.append(current_section)
                current_section = [part.strip()]  # Start new section with sample ID
            else:
                current_section.append(part)

        # Add the last section
        if current_section:
            sample_data_sections.append(current_section)

        logger.info(f"Found {len(sample_data_sections)} potential sample sections in single line")

        # Process each sample section
        for section_idx, section_parts in enumerate(sample_data_sections):
            if len(section_parts) < 2:
                continue

            sample_id = section_parts[0]
            value_parts = section_parts[1:]

            # Validate sample ID
            if not re.match(r'^[SL]\d{3,4}$', sample_id):
                continue

            # Check for numeric values
            has_numeric = False
            for part in value_parts:
                try:
                    float(part)
                    has_numeric = True
                    break
                except ValueError:
                    continue

            if not has_numeric:
                continue

            sample = {"Sample ID": sample_id}

            # Map values to expected headers
            for i in range(len(value_parts)):
                if i + 1 < len(expected_headers):  # +1 because we skip Sample ID
                    header = expected_headers[i + 1]
                    value = value_parts[i]

                    # Convert to numeric if possible
                    try:
                        if '.' in value:
                            sample[header] = float(value)
                        else:
                            sample[header] = int(value)
                    except ValueError:
                        sample[header] = value

            logger.info(f"Successfully parsed sample from single line: {sample_id}")
            samples.append(sample)

    else:
        # Multi-line parsing (existing logic)
        # Determine which lines to process based on available lines
        start_line = 1 if len(lines) >= 2 else 0  # Skip title if available

        for line_idx, line in enumerate(lines[start_line:], start=start_line):
            parts = line.split()
            logger.debug(f"Processing line {line_idx + 1}: {len(parts)} parts - {repr(line)}")

            # Skip empty lines or lines that are clearly not data
            if len(parts) < 2:
                logger.debug(f"Skipping line {line_idx + 1}: insufficient parts")
                continue

            # Look for sample ID patterns in any position
            sample_id = None
            sample_id_idx = -1

            # Check each part for sample ID pattern
            for i, part in enumerate(parts):
                if re.match(r'^[SL]\d{3,4}$', part.strip()):  # S001-S9999 or L001-L9999
                    sample_id = part.strip()
                    sample_id_idx = i
                    logger.debug(f"Found sample ID {sample_id} at position {i} in line {line_idx + 1}")
                    break

            # If no sample ID found, skip this line
            if not sample_id:
                logger.debug(f"No sample ID found in line {line_idx + 1}")
                continue

            # Extract values after the sample ID
            value_parts = parts[sample_id_idx + 1:]

            # Must have at least some values
            if not value_parts:
                logger.debug(f"No values found after sample ID in line {line_idx + 1}")
                continue

            # Check if we have numeric values
            has_numeric = False
            for part in value_parts:
                try:
                    float(part)
                    has_numeric = True
                    break
                except ValueError:
                    continue

            if not has_numeric:
                logger.debug(f"No numeric values found in line {line_idx + 1}")
                continue

            sample = {"Sample ID": sample_id}

            # Map values to expected headers (skip Sample ID header)
            for i in range(len(value_parts)):
                if i + 1 < len(expected_headers):  # +1 because we skip Sample ID
                    header = expected_headers[i + 1]
                    value = value_parts[i]

                    # Convert to numeric if possible
                    try:
                        if '.' in value:
                            sample[header] = float(value)
                        else:
                            sample[header] = int(value)
                    except ValueError:
                        sample[header] = value

            logger.info(f"Successfully parsed sample: {sample_id}")
            samples.append(sample)

    logger.info(f"Total samples parsed: {len(samples)}")

    # Convert to the expected JSON format matching the existing files
    if container_type == "soil":
        result = {"Farm_3_Soil_Test_Data": {}}
        for sample in samples:
            sample_id = sample.pop("Sample ID")  # Remove and get Sample ID
            result["Farm_3_Soil_Test_Data"][sample_id] = sample
    elif container_type == "leaf":
        result = {"Farm_3_Leaf_Test_Data": {}}
        for sample in samples:
            sample_id = sample.pop("Sample ID")  # Remove and get Sample ID
            result["Farm_3_Leaf_Test_Data"][sample_id] = sample
    else:
        result = {"type": "unknown", "samples": samples}

    return result