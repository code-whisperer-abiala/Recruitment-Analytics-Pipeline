import csv
import psycopg2
from faker import Faker
from datetime import datetime, timedelta
import random
import os
from dotenv import load_dotenv
import argparse # New import for CLI arguments

# Load environment variables from the .env file
load_dotenv()

# --- Database Configuration ---
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# --- Faker Initialization ---
fake = Faker()

# --- Predefined Data Lists ---
ROLES = ['BI Developer', 'Data Analyst', 'HR Coordinator', 'Marketing Intern', 'Recruiter', 'Software Engineer']
RECRUITERS = ['Ebube Lasisi', 'Grace Chikadibia', 'Grace Isiaka', 'Jamie Rivera', 'Jordan Kim', 'Sam Lee', 'Taylor Brooks', 'Alex Morgan', 'Chichi Bernard']

# Define the flow of statuses and valid transitions
STATUS_FLOW = {
    'Applied': ['Screening', 'Rejected'],
    'Screening': ['Interviewing', 'Rejected'],
    'Interviewing': ['Offer Sent', 'Rejected'],
    'Offer Sent': ['Hired', 'Rejected'],
    'Hired': [],
    'Rejected': []
}

# Templates for notes based on status
note_templates = {
    'Applied': [
        'Application received and under review.',
        'Submitted resume and cover letter.',
        'Initial application.',
        'Candidate expressed strong interest.'
    ],
    'Screening': [
        'Initial screening complete, proceeding to next stage.',
        'Resume reviewed, meeting minimum qualifications.',
        'Screening call scheduled.',
        'Candidate passed initial screening.'
    ],
    'Interviewing': [
        'First interview conducted. Feedback pending.',
        'Scheduled for second round interview.',
        'Interview in progress.',
        'Candidate performed well in initial interview.'
    ],
    'Offer Sent': [
        'Offer extended, awaiting response.',
        'Verbal offer made, formal offer letter sent.',
        'Negotiating terms of employment.',
        'Offer sent on good terms.'
    ],
    'Hired': [
        'Offer accepted, onboarding initiated.',
        'Successfully hired and started employment.',
        'Candidate joined the team.',
        'Onboarding completed, candidate fully integrated.'
    ],
    'Rejected': [
        'Rejected, skill mismatch.',
        'Rejected after interview, better fit found.',
        'Not moving forward at this time.',
        'Candidate withdrew application.',
        'Rejected due to experience gap.'
    ]
}

# --- Helper Function for Email Generation ---
def generate_custom_email(name):
    # Convert name to first.last format
    parts = name.lower().split()
    if len(parts) >= 2:
        first_name = parts[0]
        last_name = parts[-1]
        return f"{first_name}.{last_name}@example.com"
    elif parts:
        # Handle single-name cases by just using the name
        return f"{parts[0]}@example.com"
    return "unknown@example.com"

# --- Data Generation Functions ---
def generate_synthetic_data(num_records=100):
    candidates_data = []
    status_data = []

    for _ in range(num_records):
        # Generate Candidate Data
        name = fake.name()
        email = generate_custom_email(name) # Use the custom email function
        role_applied = random.choice(ROLES)
        recruiter = random.choice(RECRUITERS)
        application_date = fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d')

        candidates_data.append((name, email, role_applied, recruiter, application_date))

        # Simulate Status Flow for CandidateStatus
        current_status = 'Applied'
        status_history = [{'status': current_status, 'status_date': application_date}]

        while current_status not in ['Hired', 'Rejected'] and STATUS_FLOW.get(current_status):
            possible_next_statuses = STATUS_FLOW[current_status]
            if not possible_next_statuses: # No more transitions
                break
            
            # Weighted random choice (simplified, adjust weights as needed for specific distributions)
            next_status = random.choices(possible_next_statuses, k=1)[0]
            
            status_date = (datetime.strptime(status_history[-1]['status_date'], '%Y-%m-%d') + timedelta(days=random.randint(1, 15))).strftime('%Y-%m-%d')
            current_status = next_status
            status_history.append({'status': current_status, 'status_date': status_date})

            # Break if we've reached a final status or too many steps
            if current_status in ['Hired', 'Rejected'] or len(status_history) > 5:
                break
        
        # Determine the final status entry for CandidateStatus table
        final_status_entry = status_history[-1]
        
        # Add interview_date if status indicates an interview occurred
        interview_date = None
        for entry in status_history:
            if 'Interviewing' in entry['status'] or 'Offer Sent' in entry['status'] or 'Hired' in entry['status']:
                start_dt = datetime.strptime(application_date, '%Y-%m-%d')
                end_dt = datetime.strptime(final_status_entry['status_date'], '%Y-%m-%d')
                if start_dt <= end_dt:
                    interview_date = (start_dt + timedelta(days=random.randint(0, (end_dt - start_dt).days))).strftime('%Y-%m-%d')
                else:
                    interview_date = final_status_entry['status_date'] # Fallback
                break
                
        # Get notes based on final status
        notes = random.choice(note_templates.get(final_status_entry['status'], ['No specific notes.']))

        status_data.append({
            'status': final_status_entry['status'],
            'status_date': final_status_entry['status_date'],
            'interview_date': interview_date,
            'notes': notes
        })
    return candidates_data, status_data

def read_and_prepare_csv_data(csv_file_path):
    csv_candidates = []
    csv_status_details = []

    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        line_num = 1
        for row in csv_reader:
            line_num += 1
            # Sanity Checks on CSV Fields
            required_fields = ['name', 'email', 'role_applied', 'recruiter', 'application_date', 'status', 'notes']
            if not all(field in row and row[field] for field in required_fields):
                print(f"Skipping row {line_num} in CSV due to missing or empty required fields: {row}")
                # Log skipped bad rows to a separate file for debugging CSV issues
                with open("skipped_bad_csv_rows_log.txt", "a") as log:
                    log.write(f"Bad CSV row skipped (missing fields) at line {line_num}: {row}\n")
                continue

            try:
                # Prepare data for Candidates table
                candidate_info = (
                    row['name'],
                    row['email'],
                    row['role_applied'],
                    row['recruiter'],
                    row['application_date'] # Ensure this is in YYYY-MM-DD format
                )
                csv_candidates.append(candidate_info)

                # Prepare data for CandidateStatus table
                status_entry = {
                    'status': row['status'],
                    'status_date': row['application_date'], # Using application_date for simplicity here
                    'interview_date': row['interview_date'] if row['interview_date'] else None,
                    'notes': row['notes']
                }
                csv_status_details.append(status_entry)
            except KeyError as ke:
                print(f"Skipping row {line_num} in CSV due to missing column: {ke}. Row: {row}")
                with open("skipped_bad_csv_rows_log.txt", "a") as log:
                    log.write(f"Bad CSV row skipped (missing key: {ke}) at line {line_num}: {row}\n")
                continue
            except Exception as ex:
                print(f"Skipping row {line_num} in CSV due to data error: {ex}. Row: {row}")
                with open("skipped_bad_csv_rows_log.txt", "a") as log:
                    log.write(f"Bad CSV row skipped (data error: {ex}) at line {line_num}: {row}\n")
                continue

    return csv_candidates, csv_status_details

# --- Database Insertion Logic ---
def insert_all_data_into_db(csv_file_path, num_synthetic_records=100):
    conn = None
    insertion_log_file = "insertion_log.txt" # File for success logs
    skipped_duplicates_log_file = "skipped_duplicates_log.txt" # File for skipped duplicates

    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()

        # --- 1. Insert CSV Data (in its own transaction block) ---
        try:
            print(f"{datetime.now()}: Reading and inserting CSV data from {csv_file_path}...")
            with open(insertion_log_file, "a") as log:
                log.write(f"{datetime.now()}: Starting CSV data insertion from {csv_file_path}\n")

            csv_candidates_data, csv_status_details = read_and_prepare_csv_data(csv_file_path)

            inserted_csv_candidate_ids_and_indices = [] # Store (candidate_id, original_index)

            for i, candidate_tuple in enumerate(csv_candidates_data):
                email_to_insert = candidate_tuple[1]
                cur.execute(
                    "INSERT INTO Candidates (name, email, role_applied, recruiter, application_date) "
                    "VALUES (%s, %s, %s, %s, %s) "
                    "ON CONFLICT (email) DO NOTHING " # Prevents duplicate email errors
                    "RETURNING candidate_id",
                    candidate_tuple
                )
                result = cur.fetchone()
                if result:
                    inserted_csv_candidate_ids_and_indices.append((result[0], i))
                else:
                    print(f"Skipped existing CSV candidate with email: {email_to_insert}")
                    # Log Skipped Duplicates
                    with open(skipped_duplicates_log_file, "a") as log:
                        log.write(f"{datetime.now()}: Duplicate email skipped from CSV: {email_to_insert}\n")

            print(f"Inserted {len(inserted_csv_candidate_ids_and_indices)} candidates from CSV (new records).")

            # Insert statuses only for newly inserted CSV candidates
            for (candidate_id, original_index) in inserted_csv_candidate_ids_and_indices:
                status_entry = csv_status_details[original_index]
                cur.execute(
                    "INSERT INTO CandidateStatus (candidate_id, status, status_date, interview_date, notes) VALUES (%s, %s, %s, %s, %s)",
                    (candidate_id, status_entry['status'], status_entry['status_date'], status_entry['interview_date'], status_entry['notes'])
                )
            print(f"Inserted {len(inserted_csv_candidate_ids_and_indices)} candidate statuses for newly added CSV candidates.")

            conn.commit() # Commit CSV data
            print(f"{datetime.now()}: CSV data insertion committed.")
            with open(insertion_log_file, "a") as log:
                log.write(f"{datetime.now()}: CSV data insertion committed. Inserted {len(inserted_csv_candidate_ids_and_indices)} new CSV records.\n")

        except Exception as e:
            print(f"{datetime.now()}: Error inserting CSV data: {e}")
            with open(insertion_log_file, "a") as log:
                log.write(f"{datetime.now()}: ERROR - CSV data insertion failed: {e}\n")
            if conn:
                conn.rollback() # Rollback CSV data only
            # Raise exception to ensure the outer exception handler can catch it or script stops
            raise


        # --- 2. Insert Synthetic Data (in its own transaction block) ---
        print(f"{datetime.now()}: Generating and inserting {num_synthetic_records} synthetic data records...")
        with open(insertion_log_file, "a") as log:
            log.write(f"{datetime.now()}: Starting Synthetic data insertion for {num_synthetic_records} records.\n")

        try:
            synthetic_candidates_data, synthetic_statuses_data = generate_synthetic_data(num_synthetic_records)
            
            inserted_synthetic_candidate_ids_and_indices = [] # Store (candidate_id, original_index)

            for i, candidate_tuple in enumerate(synthetic_candidates_data):
                email_to_insert = candidate_tuple[1]
                cur.execute(
                    "INSERT INTO Candidates (name, email, role_applied, recruiter, application_date) "
                    "VALUES (%s, %s, %s, %s, %s) "
                    "ON CONFLICT (email) DO NOTHING " # Prevents duplicate email errors
                    "RETURNING candidate_id",
                    candidate_tuple
                )
                result = cur.fetchone()
                if result:
                    inserted_synthetic_candidate_ids_and_indices.append((result[0], i))
                else:
                    print(f"Skipped existing synthetic candidate with email: {email_to_insert}")
                    # Log Skipped Duplicates
                    with open(skipped_duplicates_log_file, "a") as log:
                        log.write(f"{datetime.now()}: Duplicate email skipped from Synthetic: {email_to_insert}\n")

            print(f"Inserted {len(inserted_synthetic_candidate_ids_and_indices)} synthetic candidates (new records).")

            # Insert statuses only for newly inserted synthetic candidates
            for (candidate_id, original_index) in inserted_synthetic_candidate_ids_and_indices:
                status_entry = synthetic_statuses_data[original_index]
                cur.execute(
                    "INSERT INTO CandidateStatus (candidate_id, status, status_date, interview_date, notes) VALUES (%s, %s, %s, %s, %s)",
                    (candidate_id, status_entry['status'], status_entry['status_date'], status_entry['interview_date'], status_entry['notes'])
                )
            print(f"Inserted {len(inserted_synthetic_candidate_ids_and_indices)} synthetic candidate statuses for newly added synthetic candidates.")

            conn.commit() # Commit Synthetic data
            print(f"{datetime.now()}: Synthetic data insertion committed.")
            with open(insertion_log_file, "a") as log:
                log.write(f"{datetime.now()}: Synthetic data insertion committed. Inserted {len(inserted_synthetic_candidate_ids_and_indices)} new synthetic records.\n")

        except Exception as e:
            print(f"{datetime.now()}: Error inserting Synthetic data: {e}")
            with open(insertion_log_file, "a") as log:
                log.write(f"{datetime.now()}: ERROR - Synthetic data insertion failed: {e}\n")
            if conn:
                conn.rollback() # Rollback Synthetic data only
            # Raise exception to ensure the outer exception handler can catch it or script stops
            raise

        print(f"{datetime.now()}: All data insertion process completed.")
        with open(insertion_log_file, "a") as log:
            log.write(f"{datetime.now()}: All data insertion process completed.\n")
        # --- Final Summary ---
        total_csv = len(inserted_csv_candidate_ids_and_indices)
        total_synthetic = len(inserted_synthetic_candidate_ids_and_indices)
        with open(insertion_log_file, "a") as log:
            log.write(f"{datetime.now()}: Summary — CSV inserted: {total_csv}, Synthetic inserted: {total_synthetic}\n")
            log.write(f"{datetime.now()}: Skipped duplicates logged in '{skipped_duplicates_log_file}'\n")
            log.write(f"{datetime.now()}: Insert completed.\n\n")

        print(f"\n DONE! Summary:")
        print(f"- CSV candidates inserted:      {total_csv}")
        print(f"- Synthetic candidates inserted: {total_synthetic}")
        print(f"- See '{skipped_duplicates_log_file}' for skipped duplicates.")
        print(f"- Full log saved to '{insertion_log_file}'\n")

    except (Exception, psycopg2.Error) as error:
        print(f"{datetime.now()}: An unexpected error occurred during overall database operations: {error}")
        with open(insertion_log_file, "a") as log:
            log.write(f"{datetime.now()}: CRITICAL ERROR - Overall process failed: {error}\n")
        print("Please ensure your Railway database is running and credentials are correct.")
        # Potential place for alerting (e.g., send_slack_notification("Critical DB error!"))
    finally:
        if conn:
            cur.close()
            conn.close()
            print(f"{datetime.now()}: PostgreSQL connection closed.")
            with open(insertion_log_file, "a") as log:
                log.write(f"{datetime.now()}: PostgreSQL connection closed.\n")

# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate candidate tracking database with CSV and synthetic data.")
    parser.add_argument("--csv_file", default="Candidates_With_Email_Updated_Notes.csv",
                        help="Path to the CSV file containing candidate data. (Default: Candidates_With_Email_Updated_Notes.csv)")
    parser.add_argument("--num_synthetic", type=int, default=100,
                        help="Number of synthetic records to generate and insert. (Default: 100)")
    args = parser.parse_args()

    # Pass the arguments to the main insertion function
    insert_all_data_into_db(args.csv_file, num_synthetic_records=args.num_synthetic)