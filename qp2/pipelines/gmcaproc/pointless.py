import os
import xml.etree.ElementTree as ET


def parse_pointless_xml(file_path):
    if os.path.exists(file_path):
        xmldata = open(file_path, "r").read()
        root = ET.fromstring(xmldata)

        # Extract information from BestCell
        best_cell = root.find("BestCell/cell")
        cell_data = {
            "a": float(best_cell.find("a").text),
            "b": float(best_cell.find("b").text),
            "c": float(best_cell.find("c").text),
            "alpha": float(best_cell.find("alpha").text),
            "beta": float(best_cell.find("beta").text),
            "gamma": float(best_cell.find("gamma").text),
        }

        # Extract information from BestSolution
        best_solution = root.find("BestSolution")

        result = {
            "GroupName": best_solution.find("GroupName").text.strip(),
            "SGnumber": best_solution.find("SGnumber").text.strip(),
            "CCP4_SGnumber": best_solution.find("CCP4_SGnumber").text.strip(),
            "ReindexOperator": best_solution.find("ReindexOperator").text.strip(),
            "ReindexMatrix": best_solution.find("ReindexMatrix").text.strip(),
            "LGProb": float(best_solution.find("LGProb").text),
            "SysAbsProb": float(best_solution.find("SysAbsProb").text),
            "Confidence": float(best_solution.find("Confidence").text),
            "LGconfidence": float(best_solution.find("LGconfidence").text),
            "TotalProb": float(best_solution.find("TotalProb").text),
            "UnitCell": " ".join(
                str(cell_data[key]) for key in ["a", "b", "c", "alpha", "beta", "gamma"]
            ),
        }
        return result


from symmetry import Symmetry


def parse_pointless_log(file_path):
    result = {
        'UnitCell': None,
        'Confidence': None,
        'GroupName': None,
        'CCP4_SGnumber': None
    }

    try:
        with open(file_path, 'r') as file:
            content = file.read()

            # Find the relevant summary section containing "Result:"
            start_marker = '<!--SUMMARY_BEGIN--> $TEXT:Result: $$ $$'
            end_marker = '$$ <!--SUMMARY_END-->'
            start_idx = content.find(start_marker)
            if start_idx == -1:
                return "Result summary section not found"
            start_idx += len(start_marker)
            end_idx = content.find(end_marker, start_idx)
            if end_idx == -1:
                return "End of result summary section not found"

            # Extract the summary section
            summary = content[start_idx:end_idx].strip()

            # Process line by line
            lines = summary.split('\n')

            for line in lines:
                line = line.strip()
                # Extract unit cell
                if line.startswith('Unit cell:'):
                    result['UnitCell'] = line.split(':', 1)[1].strip()
                # Extract space group confidence
                elif line.startswith('Space group confidence:'):
                    result['Confidence'] = line.split(':', 1)[1].strip()
                # Extract space group
                elif line.startswith('Best Solution:'):
                    print(line.split('group', 1))
                    result['GroupName'] = line.split('group', 1)[1].strip()

        result['CCP4_SGnumber'] = Symmetry.symbol_to_number(result['GroupName'])
        return result

    except FileNotFoundError:
        return "File not found"
    except Exception as e:
        return f"Error parsing file: {str(e)}"


# Example usage:
# result = parse_summary('path_to_your_file.txt')
# print(result)

r = parse_pointless_log('pointless.out')
print(r)
