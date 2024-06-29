import argparse
import json
import os
import subprocess
import urllib.parse

import requests

from src.core.config import settings
from src.db.db import Database
from src.utils.utils import delete_dir, print_hashtags, print_info, timing


class PrepareKart:
    """Clone Kart repo and setup with workingcopy in PostgreSQL"""

    def __init__(self, db, repo_url: str, maintainer: str, table_name: str):
        """Initialize class

        Args:
            db (Database): Database object
            repo_url (str): URL of the repository
            maintainer (str): Name of the maintainer (PostgreSQL user)
            table_name (str): Name of the table
        """
        self.db = db
        self.path_ssh_key = "/app/id_rsa"

        self.table_names = [
            "poi_food_drink",
            "poi_health",
            "poi_public_transport",
            "poi_other",
            "poi_mobility_service",
            "poi_public_service",
            "poi_service",
            "poi_shopping",
            "poi_sport",
            "poi_tourism_leisure",
            "poi_childcare",
            "poi_school",
        ]

        # Prepare repository URL
        self.repo_url = repo_url
        parsed_url = urllib.parse.urlparse(self.repo_url)
        self.maintainer = maintainer
        self.table_name = table_name
        # self.schema_name = f"kart_{table_name}_{maintainer}"
        self.schema_name = "kart_pois"

        # Get repo name and user name from URL
        self.repo_owner, self.repo_name = parsed_url.path.strip("/").split("/")
        self.git_domain = parsed_url.netloc
        # self.repo_ssh_url = f"git@{self.git_domain}:{self.repo_owner}/{self.repo_name}"
        self.repo_ssh_url = (
            f"https://{self.git_domain}/{self.repo_owner}/{self.repo_name}"
        )
        self.path_repo = os.path.join(
            settings.DATA_DIR, self.repo_name + "_" + self.maintainer
        )
        self.github_api_url = "https://api.github.com/repos"

    def clone_data_repo(self):
        """Clone Kart repository or pull recent changes"""

        # Check if folder exists
        if os.path.exists(self.path_repo):
            print_info(f"Folder {self.path_repo} already exists. It will be deleted.")
            # Delete folder
            delete_dir(self.path_repo)

        # Clone repo
        print_info(f"Cloning repository {self.repo_ssh_url} to {self.path_repo}.")
        subprocess.run(
            f"kart clone {self.repo_ssh_url} {self.path_repo}",
            shell=True,
            check=True,
        )
        return

    def create_new_branch(self, branch_name: str):
        """Create new branch in Kart repository

        Args:
            branch_name (str): Name of the branch
        """
        os.chdir(self.path_repo)
        subprocess.run(
            f"kart branch {branch_name}",
            shell=True,
            check=True,
        )
        print_info(f"Branch {branch_name} created")
        return

    def checkout_branch(self, branch_name: str):
        """Checkout branch in Kart repository"""
        os.chdir(self.path_repo)
        subprocess.run(
            f"kart checkout {branch_name}",
            shell=True,
            check=True,
        )
        return

    def status(self):
        """Print Kart status"""
        os.chdir(self.path_repo)
        # Return status
        status = subprocess.check_output("kart status", shell=True)
        return status.decode("utf-8")

    @timing
    def commit(self, commit_message: str):
        """Commit changes in Kart repository

        Args:
            commit_message (str): Commit message
        """
        os.chdir(self.path_repo)

        if "Nothing to commit, working copy clean" not in self.status():
            subprocess.run(
                f"kart commit -m '{commit_message}'",
                shell=True,
                check=True,
            )
        else:
            print_info("Nothing to commit, working copy clean")
        return

    @timing
    def push(self, branch_name: str):
        """Push Kart repository to remote"""
        os.chdir(self.path_repo)
        try:
            subprocess.run(
                f"kart -v push --set-upstream origin {branch_name}",
                shell=True,
                check=True,
                stderr=subprocess.PIPE,  # Capture error output
                text=True,  # Output as text, not bytes
                bufsize=1<<30,  # Increase buffer size to 1 GB
            )
        except subprocess.CalledProcessError as e:
            print(f"Error occurred while pushing to branch {branch_name}:")
            print(e.stderr)  # Print the error output of the failed command
            raise  # Re-raise the exception

    def create_pull_request(
        self,
        branch_name: str,
        base_branch: str = "main",
        title: str = "",
        body: str = "",
    ):
        # Extract owner and repo name from the repo URL
        _, _, _, owner, repo_name = self.repo_url.split("/")

        # Set the necessary authentication headers
        auth_header = {"Authorization": f"Bearer {settings.GITHUB_ACCESS_TOKEN}"}

        # Set the API endpoint and the repository information
        api_endpoint = f"{self.github_api_url}/{owner}/{repo_name}/pulls"

        # Create the payload for the API request
        payload = {
            "title": title,
            "body": body,
            "head": branch_name,
            "base": base_branch,
        }

        # Convert the payload to a JSON string
        payload_json = json.dumps(payload)

        # Make the API request to create the pull request
        response = requests.post(api_endpoint, headers=auth_header, data=payload_json)

        # Check if the request was successful
        if response.status_code == 201:
            print(f"Pull request created successfully for branch {branch_name}")
        else:
            print(
                f"Failed to create pull request for branch {branch_name}: {response.text}"
            )

    def restore(self):
        """Restore Kart repository to last commit"""
        os.chdir(self.path_repo)
        subprocess.run(
            "kart restore",
            shell=True,
            check=True,
        )
        return

    def create_schema(self):
        """Create empty kart schema in PostgreSQL and grant privileges to maintainer

        Raises:
            Exception: Schema already exists
            Exception: User does not exist (PostgreSQL user)
        """
        sql_check_user = f"""SELECT EXISTS (
            SELECT usename
            FROM pg_catalog.pg_user
            WHERE usename = '{self.maintainer}'
        );"""

        # Check if user exists
        if not self.db.select(sql_check_user)[0][0]:
            raise Exception(f"User {self.maintainer} does not exist")

        # Drop schema if it already exists
        self.db.perform(f"DROP SCHEMA IF EXISTS {self.schema_name} CASCADE")
        print_info(f"Schema {self.schema_name} deleted")

        # Create schema
        sql_create_schema = f"""CREATE SCHEMA {self.schema_name};"""
        self.db.perform(sql_create_schema)
        print_info(f"Schema {self.schema_name} created")
        # Grant privilegescd
        sql_grant_privileges = f"""
            GRANT USAGE ON SCHEMA {self.schema_name} TO {self.maintainer};
            GRANT SELECT,INSERT,UPDATE,TRUNCATE,REFERENCES,TRIGGER
            ON ALL TABLES IN SCHEMA {self.schema_name} TO {self.maintainer};
            GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA {self.schema_name} TO {self.maintainer};
            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {self.schema_name} TO {self.maintainer};
            ALTER DEFAULT PRIVILEGES IN SCHEMA {self.schema_name} GRANT SELECT,INSERT,UPDATE,TRUNCATE,REFERENCES,TRIGGER ON TABLES TO {self.maintainer};
            ALTER DEFAULT PRIVILEGES IN SCHEMA {self.schema_name} GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {self.maintainer};
            ALTER DEFAULT PRIVILEGES IN SCHEMA {self.schema_name} GRANT EXECUTE ON FUNCTIONS TO {self.maintainer};
        """
        self.db.perform(sql_grant_privileges)
        print_info(f"Privileges granted to {self.maintainer}")

    def kart_remote_workingcopy(self):
        """Create Kart remote working copy"""

        os.chdir(self.path_repo)
        # Execute command in command line to create Kart remote working copy
        subprocess.run(
            f"kart create-workingcopy postgresql://{self.db.db_config.user}:{self.db.db_config.password}@{self.db.db_config.host}{self.db.db_config.path}/{self.schema_name} --delete-existing",
            shell=True,
            check=True,
        )
        sql_owner_kart_tables = f"""
            ALTER TABLE {self.schema_name}._kart_track OWNER TO {self.maintainer};
            ALTER TABLE {self.schema_name}._kart_state OWNER TO {self.maintainer};
        """
        self.db.perform(sql_owner_kart_tables)

    def prepare_schema_kart(self):
        """Prepare a database schema for POI"""

        sql_constraints_poi_category = f"""
           ALTER TABLE {self.schema_name}.poi_categories ADD CONSTRAINT poi_category_key UNIQUE (category);
           ALTER TABLE {self.schema_name}.poi_categories ALTER COLUMN category SET NOT NULL;
           ALTER TABLE {self.schema_name}.poi_categories ALTER COLUMN table_name SET NOT NULL;
           ALTER TABLE {self.schema_name}.poi_categories ADD CONSTRAINT poi_table_name_check CHECK (table_name IN ('poi_food_drink', 'poi_health', 'poi_childcare', 'poi_public_transport', 'poi_mobility_service', 'poi_public_service', 'poi_service', 'poi_shopping', 'poi_sport', 'poi_tourism_leisure', 'poi_school', 'poi_other'));
           ALTER TABLE {self.schema_name}.poi_categories OWNER TO {self.maintainer};
        """
        sql_constraints_data_source = f"""
            ALTER TABLE {self.schema_name}.data_source ADD CONSTRAINT data_source_key UNIQUE (name);
            ALTER TABLE {self.schema_name}.data_source ALTER COLUMN name SET NOT NULL;
            ALTER TABLE {self.schema_name}.data_source ALTER COLUMN url SET NOT NULL;
            ALTER TABLE {self.schema_name}.data_source OWNER TO {self.maintainer};
        """
        self.db.perform(sql_constraints_poi_category)
        self.db.perform(sql_constraints_data_source)

        for table_name in self.table_names:
            sql_common_poi = f"""
                ALTER TABLE {self.schema_name}.{table_name}  ALTER COLUMN source SET NOT NULL;
                ALTER TABLE {self.schema_name}.{table_name}  ALTER COLUMN geom SET NOT NULL;
                ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS name_not_empty_string_check, ADD CONSTRAINT name_not_empty_string_check CHECK (name != '');
                ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS check_name_no_whitespace, ADD CONSTRAINT check_name_no_whitespace CHECK (name = LTRIM(RTRIM(name)) AND name <> '');
                ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS check_street_no_whitespace, ADD CONSTRAINT check_street_no_whitespace CHECK (street = LTRIM(RTRIM(street)) AND street <> '');
                ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS street_not_empty_string_check, ADD CONSTRAINT street_not_empty_string_check CHECK (street != '');
                ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS housenumber_not_empty_string_check, ADD CONSTRAINT housenumber_not_empty_string_check CHECK (housenumber != '');
                ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS zipcode_not_empty_string_check, ADD CONSTRAINT zipcode_not_empty_string_check CHECK (zipcode != '');
                ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS phone_not_empty_string_check, ADD CONSTRAINT phone_not_empty_string_check CHECK (phone != '');
                ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS email_check, ADD CONSTRAINT email_check CHECK (octet_length(email) BETWEEN 6 AND 320 AND email LIKE '_%@_%.__%');
                ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS opening_hours_not_empty_string_check, ADD CONSTRAINT opening_hours_not_empty_string_check CHECK (opening_hours != '');
                ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS website_not_empty_string_check, ADD CONSTRAINT website_not_empty_string_check CHECK (website != '');
                ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS wheelchair_check, ADD CONSTRAINT wheelchair_check CHECK (wheelchair IN ('yes', 'no', 'limited'));
                ALTER TABLE {self.schema_name}.{table_name}  ADD FOREIGN KEY (source) REFERENCES {self.schema_name}.data_source(name) ON DELETE CASCADE;
                ALTER TABLE {self.schema_name}.{table_name}  OWNER TO {self.maintainer};
            """
            self.db.perform(sql_common_poi)


            # Add additional constraints all tables besides poi_childcare and poi_school
            if table_name not in ("poi_childcare", "poi_school"):
                sql_addition_constraints = f"""
                    ALTER TABLE {self.schema_name}.{table_name}  ALTER COLUMN category SET NOT NULL;
                    ALTER TABLE {self.schema_name}.{table_name}  ADD FOREIGN KEY (category) REFERENCES {self.schema_name}.poi_categories(category) ON DELETE CASCADE;
                    ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS other_categories_array_check, ADD CONSTRAINT other_categories_array_check CHECK (other_categories IS NULL OR other_categories::text[] IS NOT NULL);
                    ALTER TABLE {self.schema_name}.{table_name}  OWNER TO {self.maintainer};
                """
                self.db.perform(sql_addition_constraints)

            # Add additional constraint for operator and tags column to all tables besides poi_childcare
            if table_name not in ("poi_childcare"):
                sql_addition_constraints_operator_tags = f"""
                    ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS operator_not_empty_string_check,  ADD CONSTRAINT operator_not_empty_string_check CHECK (operator != '');
                    ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS tags_jsonb_check, ADD CONSTRAINT tags_jsonb_check CHECK (jsonb_typeof(tags::jsonb) = 'object');
                    ALTER TABLE {self.schema_name}.{table_name}  OWNER TO {self.maintainer};
            """
                self.db.perform(sql_addition_constraints_operator_tags)

            # Add additional constraints for poi_childcare
            if table_name in ("poi_childcare"):
                sql_addition_constraints_childcare = f"""
                    ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS min_age_check, ADD CONSTRAINT min_age_check CHECK (min_age IS NULL OR (min_age >= 0 AND min_age <= 16));
                    ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS max_age_check, ADD CONSTRAINT max_age_check CHECK (max_age IS NULL OR (max_age >= 0 AND max_age <= 16));
                    ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS carrier_not_empty_string_check, ADD CONSTRAINT carrier_not_empty_string_check CHECK (carrier != '');
                    ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS carrier_type_not_empty_string_check, ADD CONSTRAINT carrier_type_not_empty_string_check CHECK (carrier_type != '');
                    ALTER TABLE {self.schema_name}.{table_name}  DROP CONSTRAINT IF EXISTS min_max_check, ADD CONSTRAINT min_max_check CHECK (min_age <= max_age);
                    ALTER TABLE {self.schema_name}.{table_name}  OWNER TO {self.maintainer};
                """
                print(f"Executing SQL: {sql_addition_constraints_childcare}")

                self.db.perform(sql_addition_constraints_childcare)

    def prepare_kart(self):
        """Run all functions to prepare Kart for POI data"""
        self.clone_data_repo()
        self.create_schema()
        self.kart_remote_workingcopy()
        self.prepare_schema_kart()


def parse_args(args=None):
    # define the flags and their default values
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo_url", required=True, help="URL of the repository")
    parser.add_argument("--maintainer", required=True, help="Name of the maintainer")
    parser.add_argument("--table_name", required=True, help="Name of the table")

    # parse the command line arguments
    return parser.parse_args(args)

@timing
def main():
    args = parse_args()
    repo_url = args.repo_url
    maintainer = args.maintainer
    table_name = args.table_name

    print_hashtags()
    print_info("Start Prepare Kart")
    print_hashtags()

    # Check if table is supported
    supported_tables = ["poi"]
    if table_name not in supported_tables:
        raise Exception("Table name not supported")

    # Init db and class
    db = Database(settings.LOCAL_DATABASE_URI)
    prepare_kart = PrepareKart(
        db, repo_url=repo_url, maintainer=maintainer, table_name=table_name
    )

    prepare_kart.prepare_kart()

    print_hashtags()
    print_info("End Prepare Kart")
    print_hashtags()


if __name__ == "__main__":
    main()
