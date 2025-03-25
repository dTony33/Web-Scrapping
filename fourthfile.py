#app/account_automation/services/job_control_service.py
"""
Service for controlling job execution via database flags.
"""
from typing import Dict, Any, List, Optional
from datetime import datetime


class JobControlService:
    """
    Service for checking if jobs are enabled or disabled via database flags.
    
    This service provides a centralized mechanism for enabling and disabling jobs
    through database flags rather than code changes.
    """
    
    def __init__(self, db, logger, query_repository):
        """
        Initialize the job control service.
        
        Args:
            db: Database connection instance
            logger: Logger instance for logging job control operations
            query_repository: Repository for SQL queries
        """
        self.db = db
        self.logger = logger
        self.query_repository = query_repository
        
        # Ensure the job control table exists
        self._ensure_table_exists()
    
    def is_job_enabled(self, job_name: str, region: Optional[str] = None) -> bool:
        """
        Check if a specific job is enabled.
        
        Args:
            job_name: Name of the job to check
            region: Optional region to check (for region-specific control)
            
        Returns:
            bool: True if job is enabled, False otherwise
        """
        query = self.query_repository.get_query('get_job_control')
        
        try:
            params = [job_name, region or '']
            result = self.db.execute_param(query, params)
            
            if result and len(result) > 0:
                row = result[0]
                is_enabled = bool(row.IsEnabled)
                self.logger.log(f"Job control check for {job_name} in {region or 'all regions'}: {'enabled' if is_enabled else 'disabled'}")
                return is_enabled
            else:
                # If no record found, default to enabled
                self.logger.log(f"No job control record found for {job_name} in {region or 'all regions'}, defaulting to enabled")
                return True
        except Exception as e:
            self.logger.error(f"Error checking job control status: {str(e)}")
            # On error, default to disabled for safety
            return False
    
    def set_job_enabled(self, job_name: str, enabled: bool, region: Optional[str] = None, 
                      updated_by: str = "System", comments: Optional[str] = None) -> bool:
        """
        Enable or disable a specific job.
        
        Args:
            job_name: Name of the job to update
            enabled: True to enable, False to disable
            region: Optional region for region-specific control
            updated_by: User or system that made the change
            comments: Optional comments about the change
            
        Returns:
            bool: True if update was successful, False otherwise
        """
        try:
            # Check if record exists
            check_query = self.query_repository.get_query('check_job_control_exists')
            exists_result = self.db.execute_param(check_query, [job_name, region or ''])
            
            exists = exists_result and len(exists_result) > 0
            
            if exists:
                # Update existing record
                query = self.query_repository.get_query('update_job_control')
                params = [1 if enabled else 0, updated_by, comments or '', job_name, region or '']
                self.db.execute_param(query, params)
            else:
                # Insert new record
                query = self.query_repository.get_query('insert_job_control')
                params = [job_name, region or '', 1 if enabled else 0, updated_by, comments or '']
                self.db.execute_param(query, params)
            
            action = "enabled" if enabled else "disabled"
            region_text = f" in {region}" if region else ""
            self.logger.log(f"Job {job_name}{region_text} {action} by {updated_by}")
            
            return True
        except Exception as e:
            self.logger.error(f"Error updating job control status: {str(e)}")
            return False
    
    def get_job_statuses(self, region: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get the status of all jobs or jobs for a specific region.
        
        Args:
            region: Optional region to filter by
            
        Returns:
            List of job status dictionaries
        """
        try:
            if region:
                query = self.query_repository.get_query('get_job_controls_by_region')
                result = self.db.execute_param(query, [region])
            else:
                query = self.query_repository.get_query('get_all_job_controls')
                result = self.db.query_all(query)
            
            job_statuses = []
            for row in result:
                job_statuses.append({
                    "job_name": row.JobName,
                    "region": row.Region,
                    "enabled": bool(row.IsEnabled),
                    "last_updated_by": row.LastUpdatedBy,
                    "last_updated_on": row.LastUpdatedOn.isoformat() if hasattr(row, 'LastUpdatedOn') else None,
                    "comments": row.Comments
                })
            
            return job_statuses
        except Exception as e:
            self.logger.error(f"Error getting job statuses: {str(e)}")
            return []
    
    def _ensure_table_exists(self) -> None:
        """
        Ensure the JobControl table exists.
        
        This method checks if the JobControl table exists and creates it if it doesn't.
        """
        try:
            # Check if table exists
            check_query = """
            SELECT OBJECT_ID('JobControl') AS TableID
            """
            result = self.db.query_single(check_query)
            
            if not result or not result.TableID:
                # Create table
                create_query = """
                CREATE TABLE JobControl (
                    Id INT IDENTITY(1,1) PRIMARY KEY,
                    JobName NVARCHAR(100) NOT NULL,
                    Region NVARCHAR(50) NULL,
                    IsEnabled BIT NOT NULL DEFAULT 1,
                    LastUpdatedBy NVARCHAR(100) NOT NULL,
                    LastUpdatedOn DATETIME NOT NULL DEFAULT GETDATE(),
                    Comments NVARCHAR(500) NULL
                );
                
                -- Create index for fast lookups
                CREATE INDEX IX_JobControl_JobName_Region ON JobControl(JobName, Region);
                """
                self.db.execute(create_query)
                self.logger.log("Created JobControl table")
                
                # Insert default job control records
                default_jobs = [
                    ('dda_threshold_p', None, 1, 'System', 'Default setting for Personal DDA Threshold Job'),
                    ('dda_threshold_b', None, 1, 'System', 'Default setting for Business DDA Threshold Job'),
                    ('dda_mining_p', None, 1, 'System', 'Default setting for Personal DDA Mining Job'),
                    ('dda_mining_b', None, 1, 'System', 'Default setting for Business DDA Mining Job'),
                    ('dda_sdg_p', None, 1, 'System', 'Default setting for Personal DDA SDG Job'),
                    ('dda_sdg_b', None, 1, 'System', 'Default setting for Business DDA SDG Job'),
                    ('cca_threshold_p', None, 1, 'System', 'Default setting for Personal CCA Threshold Job'),
                    ('cca_threshold_b', None, 1, 'System', 'Default setting for Business CCA Threshold Job'),
                    ('cca_mining', None, 1, 'System', 'Default setting for CCA Mining Job'),
                    ('cca_sdg_p', None, 1, 'System', 'Default setting for Personal CCA SDG Job'),
                    ('cca_sdg_b', None, 1, 'System', 'Default setting for Business CCA SDG Job'),
                    ('im80', None, 1, 'System', 'Default setting for IM80 Transaction Job')
                ]
                
                insert_query = """
                INSERT INTO JobControl (JobName, Region, IsEnabled, LastUpdatedBy, Comments)
                VALUES (?, ?, ?, ?, ?)
                """
                for job in default_jobs:
                    self.db.execute_param(insert_query, job)
                
                self.logger.log("Inserted default job control records")
        except Exception as e:
            self.logger.error(f"Error ensuring JobControl table exists: {str(e)}")


# app/account_automation/repositories/query_repository.py
"""
Repository for storing and retrieving SQL queries.
"""
from typing import Dict, Any


class QueryRepository:
    """
    Repository for storing and retrieving SQL queries.
    
    This class centralizes SQL queries to improve maintainability
    and avoid hardcoded queries throughout the codebase.
    """
    
    def __init__(self):
        """Initialize the query repository with predefined queries."""
        self._queries = {
            # Account queries
            'get_account_by_number': """
                SELECT * FROM ReservedData WHERE AccountNumber = ?
            """,
            
            # AutoSignalData queries
            'get_sit1_signal_data': """
                SELECT * FROM AutoSignalData WHERE SKey = 'RES Create DDA' AND GroupName = 'RES'
            """,
            
            'get_sit2_signal_data': """
                SELECT * FROM AutoSignalData WHERE SKey = 'RES Create DDA SIT2' AND GroupName = 'RES'
            """,
            
            # Region queries
            'get_regions': """
                EXEC [dbo].[UspPreReserveRegions]
            """,
            
            # DDA queries
            'get_dda_count': """
                SELECT COUNT(*) 
                FROM ReservedData 
                WHERE DCA IS NULL 
                AND ProdCode = ? 
                AND Region = ? 
                AND DataSource = ? 
                AND Status = 'NEW'
            """,
            
            # DDA threshold queries
            'get_account_count': """
                SELECT COUNT(*) 
                FROM ReservedData 
                WHERE DCA IS NULL 
                AND ProdCode = '{prod_code}' 
                AND Region = '{region}' 
                AND DataSource = '{datasource}' 
                AND Status = 'NEW'
            """,
            
            # CCA queries
            'get_cca': """
                SELECT * FROM ReservedData 
                WHERE Status='New' AND ProdCode = ? 
                AND AccountNumber IS NOT NULL AND Region = ?
            """,
            
            'update_cca_query': """
                UPDATE ReservedData SET CCA = ? WHERE Id = ?
            """,
            
            'update_not_progress': """
                UPDATE ReservedData SET Status = 'New', UpdatedOn = GETDATE() WHERE Id = ?
            """,
            
            'update_progress': """
                UPDATE ReservedData SET Status = 'In Progress', UpdatedOn = GETDATE() WHERE Id = ?
            """,
            
            # IM80 queries
            'update_im80_status': """
                UPDATE ReservedData SET IM80Status = ? WHERE Id = ?
            """,
            
            'get_im80_candidates_p': """
                SELECT TOP (?) *
                FROM ReservedData 
                WHERE Region = ? 
                  AND ProdCode IN ('PER', 'CD1', 'CC1')
                  AND (IM80Status IS NULL OR JSON_VALUE(IM80Status, '$.status') = 'failed')
                  AND Status = 'NEW'
                ORDER BY NEWID()
            """,
            
            'get_im80_candidates_b': """
                SELECT TOP (?) *
                FROM ReservedData 
                WHERE Region = ? 
                  AND ProdCode IN ('BUS', 'CD2', 'CC2')
                  AND (IM80Status IS NULL OR JSON_VALUE(IM80Status, '$.status') = 'failed')
                  AND Status = 'NEW'
                ORDER BY NEWID()
            """,
            
            # Job control queries
            'get_job_control': """
                SELECT IsEnabled, LastUpdatedBy, LastUpdatedOn, Comments
                FROM JobControl 
                WHERE JobName = ? AND (Region = ? OR Region IS NULL)
                ORDER BY Region DESC
            """,
            
            'check_job_control_exists': """
                SELECT 1 FROM JobControl 
                WHERE JobName = ? AND (Region = ? OR (Region IS NULL AND ? = ''))
            """,
            
            'update_job_control': """
                UPDATE JobControl 
                SET IsEnabled = ?, LastUpdatedBy = ?, LastUpdatedOn = GETDATE(), Comments = ?
                WHERE JobName = ? AND (Region = ? OR (Region IS NULL AND ? = ''))
            """,
            
            'insert_job_control': """
                INSERT INTO JobControl (JobName, Region, IsEnabled, LastUpdatedBy, Comments)
                VALUES (?, ?, ?, ?, ?)
            """,
            
            'get_all_job_controls': """
                SELECT * FROM JobControl ORDER BY JobName, Region
            """,
            
            'get_job_controls_by_region': """
                SELECT * FROM JobControl 
                WHERE Region = ? OR Region IS NULL
                ORDER BY JobName, Region
            """,
            
            # Insert account queries
            'insert_account_query': """
                INSERT INTO [dbo].[ReservedData]
                    ([EntityCode]
                    ,[PartyID]
                    ,[AccountNumber]
                    ,[CCA]
                    ,[CreatedBy]
                    ,[CreatedOn]
                    ,[Status]
                    ,[Consumed]
                    ,[Region]
                    ,[ProdCode])
                VALUES
                    (?,?,?,?,?,GETDATE(),'New','N',?,?)
            """,
            
            'insert_account_query_no_scriptname': """
                INSERT INTO [dbo].[ReservedData]
                    ([EntityCode]
                    ,[PartyID]
                    ,[AccountNumber]
                    ,[CreatedBy]
                    ,[CreatedOn]
                    ,[Status]
                    ,[Consumed]
                    ,[Region]
                    ,[ProdCode]{column})
                VALUES
                    (?,?,?,?,GETDATE(),?,?,?,?{value})
            """,
            
            # For getting configuration from signal data
            'get_signal_data': """
                SELECT * FROM AutoSignalData WHERE SKey = ? AND GroupName = ?
            """
        }
    
    def get_query(self, query_name: str) -> str:
        """
        Get a query by name.
        
        Args:
            query_name: Name of the query to retrieve
            
        Returns:
            str: SQL query text
            
        Raises:
            KeyError: If query not found
        """
        if query_name in self._queries:
            return self._queries[query_name]
        else:
            raise KeyError(f"Query '{query_name}' not found")
    
    def register_query(self, query_name: str, query_text: str) -> None:
        """
        Register a new query or override an existing one.
        
        Args:
            query_name: Name of the query to register
            query_text: SQL query text
        """
        self._queries[query_name] = query_text
    
    def get_im80_candidates_query(self, account_type: str) -> str:
        """
        Get the query for IM80 transaction candidates based on account type.
        
        Args:
            account_type: P for Personal, B for Business
            
        Returns:
            str: SQL query text
        """
        if account_type == 'P':
            return self._queries['get_im80_candidates_p']
        else:
            return self._queries['get_im80_candidates_b']

#app/account_automation/services/account_service.py
"""
Service for account-related operations.
"""
import json
from datetime import date, timedelta
from random import randrange
from typing import Dict, Any, List, Optional, Tuple


class AccountService:
    """
    Service for account-related operations.
    
    This service handles creation and management of accounts (DDA, DCA, CCA)
    from both mining and synthetic data sources.
    """
    
    def __init__(self, db, logger, config, customer_service, inquiry_service, api_client, query_repository):
        """
        Initialize the account service.
        
        Args:
            db: Database connection instance
            logger: Logger instance for logging account operations
            config: Configuration instance for settings
            customer_service: Customer service for customer operations
            inquiry_service: Inquiry service for inquiry operations
            api_client: API client for external service calls
            query_repository: Repository for SQL queries
        """
        self.db = db
        self.logger = logger
        self.config = config
        self.customer_service = customer_service
        self.inquiry_service = inquiry_service
        self.api_client = api_client
        self.query_repository = query_repository
    
    # === DDA (Demand Deposit Account) Operations ===
    
    def create_dda_synthetic(self, customer_type: str, region: str, product_type_alpha: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a synthetic DDA account.
        
        Args:
            customer_type: Customer type (P for Personal, B for Business)
            region: Region code (SIT1, SIT2, etc.)
            product_type_alpha: Optional specific product type
            
        Returns:
            Created account data
            
        Raises:
            Exception: If account creation fails
        """
        self.logger.log(f"Creating synthetic DDA account for {customer_type} in {region}")
        
        # Get state code
        state_codes = self.config.get('state_code', ['NNC', 'NFL', 'NGA', 'NTX'])
        state_code = state_codes[randrange(len(state_codes))]
        
        # Get product type alpha from config
        type_alpha_options = self.config.get_product_types(customer_type, region)
            
        # Use provided product type or select randomly
        if product_type_alpha and product_type_alpha in type_alpha_options:
            type_alpha = product_type_alpha
        else:
            type_alpha = type_alpha_options[randrange(len(type_alpha_options))]
        
        # Entity code
        entity = 'N' + state_code
        
        # Get demographics
        demographics_query = self.config.get('get_demographics_query', '').format(StateCode=state_code)
        demographics = self.db.query_single(demographics_query)
        
        # Get customer name
        cust_name_query = self.config.get('get_cust_name_query', '')
        cust_name = self.db.query_single(cust_name_query)
        
        # Map region to test region
        test_region = 'I31' if region == 'SIT1' else 'I32'
        
        # Prepare payload for DDA creation
        payload = self.config.get('sdg_dda_payload', {}).copy()
        
        # Set customer data based on customer type
        if customer_type == 'P':
            # Personal customer
            payload['Customer']['Personal']['FirstName'] = cust_name.CustFirstName.strip()
            payload['Customer']['Personal']['LastName'] = cust_name.CustLastName.strip()
        else:
            # Business customer
            payload['Account']['ProductTypeAlpha'] = "BFUN"
            payload['UseCaseId'] = "10"
            payload['Customer']['Business']['OrgName'] = cust_name.CustFirstName.strip()
        
        # Set common customer data
        payload['Customer']['AddressLine1'] = demographics.Address
        payload['Customer']['City'] = demographics.City
        payload['Customer']['State'] = state_code
        payload['Customer']['ZipCode'] = demographics.ZipCode
        payload['Customer']['ProductEntity'] = entity
        payload['Customer']['ProductTypeAlpha'] = type_alpha
        payload['Customer']['Region'] = test_region
        payload['Customer']['UserId'] = self.config.get('service_id')
        
        # Create DDA via API
        response = self.api_client.create_dda(payload)
        
        if response['status'] == 'success':
            # Extract account data
            account_data = response['data']['DepositAccounts']['Accounts'][0]
            customer_data = response['data']['Customer']
            
            # Insert into database
            insert_query = self.config.get('insert_account_query_no_scriptname').format(
                EntityCode=account_data['EntityCode'],
                PartyID=account_data['PartyId'],
                AccountNumber=account_data['AccountNumber'],
                serviceId=self.config.get('service_id'),
                status='New',
                Region=region,
                ProdCode=account_data['ProductCode'],
                column=',DataSource',  # Add DataSource column
                value=",'SDG'"  # Mark as synthetic data
            )
            
            self.db.write(insert_query)
            
            self.logger.log(f"DDA account created successfully: {account_data['AccountNumber']}")
            return response['data']
        else:
            error_message = response.get('error', 'Unknown error')
            self.logger.error(f"DDA creation failed: {error_message}")
            raise Exception(f"DDA creation failed: {error_message}")
    
    def create_dda_mining(self, customer_type: str, region: str) -> str:
        """
        Create a DDA account from mining source.
        
        Args:
            customer_type: Customer type (P for Personal, B for Business)
            region: Region code (SIT1, SIT2, etc.)
            
        Returns:
            Account number of created account
            
        Raises:
            Exception: If account creation fails
        """
        self.logger.log(f"Creating mining DDA account for {customer_type} in {region}")
        
        # Determine test region code
        test_region = "I31" if region == "SIT1" else "I32"
        
        # Prepare XML for mining query
        if customer_type == 'P':
            xml_template = self.config.get('base_xml_per')
            prod_code = 'PER'
        else:
            xml_template = self.config.get('base_xml_bus')
            prod_code = 'BUS'
        
        # Get product type alpha options from config
        prod_type_options = self.config.get_product_types(customer_type, region)
        
        # Select random product type alpha
        selected_type = prod_type_options[randrange(len(prod_type_options))]
        
        # Prepare XML by replacing placeholders
        import xml.etree.ElementTree as ET
        xml_root = ET.fromstring(xml_template)
        
        # Find and update product type alpha element (assuming it exists in the XML)
        for elem in xml_root.findall(".//ATTRIBUTE_DEF/ATTRIBUTE_NAME[text()='Product Type Alpha']/.."):
            value_elem = elem.find("ATTRIBUTE_VALUE")
            if value_elem is not None:
                value_elem.text = selected_type
        
        # Convert back to string
        import xml.dom.minidom
        xml_str = xml.dom.minidom.parseString(ET.tostring(xml_root)).toprettyxml()
        
        # Maps for QARE API
        maps = {
            'Mining2': {
                'scan_id': '1',
                'prod_cd': prod_code,
                'miningIdentifier': 'Mining',
                'status': 'New',
                'elem_map': {
                    'ProductTypeAlpha': 'ProductTypeAlpha'
                }
            }
        }
        
        # Query and reserve account via inquiry service
        account_number = self.inquiry_service.multiple_products_mining(
            xml_query=xml_str,
            region=test_region,
            profile_id=None,
            maps=maps,
            reservation_days=365
        )
        
        # Get account details from database
        query = f"SELECT * FROM ReservedData WHERE AccountNumber = '{account_number}'"
        row = self.db.query_single(query)
        
        if not row:
            raise Exception(f"Account {account_number} not found in database after reservation")
        
        # Create customer based on customer type
        if customer_type == "P":
            new_pid = self.customer_service.create_personal_customer(row.EntityCode, region)
        else:
            new_pid = self.customer_service.create_business_customer(row.EntityCode, region)
        
        self.logger.log(f"Customer created: {new_pid}")
        
        # Link customer to account
        self.customer_service.link_customer_to_account(
            party_id=new_pid,
            account_number=row.AccountNumber,
            prod_code=row.ProdCode,
            entity=row.EntityCode,
            region=region,
            relationship_code="OWN"
        )
        
        # Update party ID in database
        update_query = f"UPDATE ReservedData SET PartyID = '{new_pid}' WHERE Id = {row.Id}"
        self.db.write(update_query)
        
        # Get customer details
        customer_data = self.customer_service.get_customer_details(new_pid, region)
        
        # Extract customer name based on customer type
        if customer_type == "P":
            first_name = customer_data.get('firstName', '')
            last_name = customer_data.get('lastName', '')
        else:
            first_name = customer_data.get('nameText', '')
            last_name = ''
        
        # Update customer name in database
        name_query = f"UPDATE ReservedData SET FirstName = '{first_name}', LastName = '{last_name}' WHERE Id = {row.Id}"
        self.db.write(name_query)
        
        self.logger.log(f"DDA account created and linked to customer: {account_number}")
        return account_number
    
    # === DCA (Certificate of Deposit Account) Operations ===
    
    def create_dca_synthetic(self, customer_type: str, region: str, product_type_alpha: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a synthetic DCA (Certificate of Deposit) account.
        
        Args:
            customer_type: Customer type (P for Personal, B for Business)
            region: Region code (SIT1, SIT2, etc.)
            product_type_alpha: Optional specific product type
            
        Returns:
            Created account data
            
        Raises:
            Exception: If account creation fails
        """
        self.logger.log(f"Creating synthetic DCA account for {customer_type} in {region}")
        
        # Implementation would be similar to create_dda_synthetic but with DCA-specific logic
        # Note: This is a placeholder implementation
        
        return {"status": "not_implemented", "message": "DCA synthetic account creation not fully implemented"}
    
    def create_dca_mining(self, customer_type: str, region: str) -> str:
        """
        Create a DCA account from mining source.
        
        Args:
            customer_type: Customer type (P/B)
            region: Region code (SIT1, SIT2, etc.)
            
        Returns:
            Account number of created account
            
        Raises:
            Exception: If account creation fails
        """
        self.logger.log(f"Creating mining DCA account for {customer_type} in {region}")
        
        # Implementation would be similar to create_dda_mining but with DCA-specific logic
        # Note: This is a placeholder that returns a mock account number
        
        return "DCA_PLACEHOLDER_ACCOUNT"
    
    # === CCA (Credit Card Account) Operations ===
    
    def create_cca_synthetic(self, customer_type: str, region: str, product_type_alpha: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a synthetic CCA (Credit Card Account).
        
        Args:
            customer_type: Customer type (P for Personal, B for Business)
            region: Region code (SIT1, SIT2, etc.)
            product_type_alpha: Optional specific product type
            
        Returns:
            Created account data
            
        Raises:
            Exception: If account creation fails
        """
        self.logger.log(f"Creating synthetic CCA account for {customer_type} in {region}")
        
        # Get product type alpha for CCA
        if customer_type == 'P':
            type_alpha_options = self.config.get('prod_type_alpha_cca_per', ['VISA', 'MCRD', 'AMEX'])
        else:
            type_alpha_options = self.config.get('prod_type_alpha_cca_bus', ['BVSA', 'BMCR'])
            
        # Use provided product type or select randomly
        if product_type_alpha and product_type_alpha in type_alpha_options:
            type_alpha = product_type_alpha
        else:
            type_alpha = type_alpha_options[randrange(len(type_alpha_options))]
        
        # Get state code
        state_codes = self.config.get('state_code', ['NNC', 'NFL', 'NGA', 'NTX'])
        state_code = state_codes[randrange(len(state_codes))]
        
        # Entity code
        entity = 'N' + state_code
        
        # Prepare payload for CCA creation
        payload = self.config.get('create_cca_payload', {}).copy()
        payload['CardType'] = self.config.get('card_type', 'VISA')
        payload['CreditLineAmount'] = self.config.get('credit_line_amount', '3000')
        payload['CashCreditLine'] = self.config.get('cash_credit_line', '2000')
        payload['CustomerEntity'] = entity
        payload['Region'] = region
        payload['UserId'] = self.config.get('service_id')
        
        # Create CCA via API
        response = self.api_client.create_cca(payload)
        
        if response['status'] == 'success':
            # Extract account data
            account_data = response['data']['Account']
            
            # Insert into database
            insert_query = self.config.get('insert_account_query').format(
                EntityCode=entity,
                PartyID=account_data['PartyId'],
                AccountNumber='',  # CCA doesn't have traditional account number
                CCA=account_data['AccountNumber'],  # Credit card number goes in CCA field
                serviceId=self.config.get('service_id'),
                Region=region
            )
            
            self.db.write(insert_query)
            
            self.logger.log(f"CCA account created successfully: {account_data['AccountNumber']}")
            return response['data']
        else:
            error_message = response.get('error', 'Unknown error')
            self.logger.error(f"CCA creation failed: {error_message}")
            raise Exception(f"CCA creation failed: {error_message}")
    
    def create_cca_mining(self, party_id: str, region: str) -> str:
        """
        Create a CCA account from mining source or using provided party ID.
        
        Args:
            party_id: Party ID to associate with the account (empty string to use mining)
            region: Region code (SIT1, SIT2, etc.)
            
        Returns:
            Account number of created account
            
        Raises:
            Exception: If account creation fails
        """
        self.logger.log(f"Creating CCA account for party ID {party_id} in region {region}")
        
        cca = None
        
        # If party ID is provided, create CCA for that party
        if party_id:
            # Get state code
            state_codes = self.config.get('state_code', ['NNC', 'NFL', 'NGA', 'NTX'])
            cust_entity = state_codes[randrange(len(state_codes))]
            
            # Prepare payload for CCA creation
            payload = self.config.get('create_cca_payload', {}).copy()
            payload['PartyId'] = party_id
            payload['CardType'] = self.config.get('card_type', 'VISA')
            payload['CreditLineAmount'] = self.config.get('credit_line_amount', '3000')
            payload['CashCreditLine'] = self.config.get('cash_credit_line', '2000')
            payload['Region'] = region
            payload['UserId'] = self.config.get('service_id')
            payload['CustomerEntity'] = cust_entity
        else:
            # Use mining to get a CCA
            get_cca_query = self.config.get('get_cca_query', '').replace("#Region#", region)
            cca = self.db.query_single(get_cca_query)
            
            if not cca:
                raise Exception("CCA not found in mining")
                
            id = cca.ID
            
            # Prepare payload for CCA creation
            payload = self.config.get('create_cca_payload', {}).copy()
            payload['PartyId'] = cca.PartyId
            payload['CardType'] = self.config.get('card_type', 'VISA')
            payload['CreditLineAmount'] = self.config.get('credit_line_amount', '3000')
            payload['CashCreditLine'] = self.config.get('cash_credit_line', '2000')
            payload['Region'] = region
            payload['UserId'] = self.config.get('service_id')
            payload['CustomerEntity'] = cca.EntityCode
        
        # Create CCA via API
        response = self.api_client.create_cca(payload)
        
        if response['status'] == 'success':
            account_data = response['data']['Account']
            ccaCard = account_data['AccountNumber']
            ccaParty = account_data['PartyId']
            
            if not party_id and cca:
                # Update existing record
                cca_update_query = self.config.get('update_cca_query', '').format(ID=cca.Id, CCA=ccaCard)
                self.db.write(cca_update_query)
                update_query = self.query_repository.get_query('update_not_progress')
                self.db.execute_param(update_query, [id])
            else:
                # Insert new record
                insert_query = self.config.get('insert_account_query').format(
                    EntityCode=payload['CustomerEntity'],
                    PartyID=ccaParty,
                    AccountNumber='',  # CCA doesn't have traditional account number
                    CCA=ccaCard,
                    Region=region,
                    serviceId=self.config.get('service_id')
                )
                self.db.write(insert_query)
            
            self.logger.log(f"CCA account created successfully: {ccaCard}")
            return ccaCard
        else:
            # Handle failure
            if cca and not party_id:
                update_query = self.query_repository.get_query('update_not_progress')
                self.db.execute_param(update_query, [id])
                
            error_message = response.get('error', 'Unknown error')
            self.logger.error(f"CCA creation failed: {error_message}")
            raise Exception(f"CCA creation failed: {error_message}")
    
    # === Helper Methods ===
    
    def get_threshold(self, account_type: str, customer_type: str, region: str, datasource: str) -> int:
        """
        Calculate how many accounts need to be created based on thresholds.
        
        Args:
            account_type: Type of account (dda, dca, cca)
            customer_type: Type of customer (P for Personal, B for Business)
            region: Region code (SIT1, SIT2, etc.)
            datasource: Data source (Mining, SDG)
            
        Returns:
            Number of accounts to create
        """
        self.logger.log(f"Calculating threshold for {account_type} {customer_type} accounts in {region} from {datasource}")
        
        # Map account types to product codes
        product_codes = {
            'dda': {'P': 'PER', 'B': 'BUS'},
            'dca': {'P': 'CD1', 'B': 'CD2'},
            'cca': {'P': 'CC1', 'B': 'CC2'}
        }
        
        # Get product code
        prod_code = product_codes.get(account_type, {}).get(customer_type)
        if not prod_code:
            raise ValueError(f"Invalid account type '{account_type}' or customer type '{customer_type}'")
        
        # Build query to get existing accounts
        where_clauses = [
            "DCA is null",  # Not a debit card account
            f"Region = '{region}'",
            f"DataSource = '{datasource}'",
            f"ProdCode = '{prod_code}'",
            "Status = 'NEW'"
        ]
        
        query = f"""
        SELECT COUNT(*) FROM ReservedData 
        WHERE {' AND '.join(where_clauses)}
        """
        
        # Get existing accounts count
        result = self.db.execute_scalar(query)
        new_count = result if result is not None else 0
        
        # Get threshold from configuration
        customer_prefix = 'per' if customer_type == 'P' else 'bus'
        max_account = self.config.get_threshold(customer_type, datasource, region)
        
        # Calculate how many accounts to create
        accounts_to_create = max_account - new_count
        
        if accounts_to_create > 0:
            self.logger.log(f"Need to create {accounts_to_create} {account_type} accounts for {customer_type} from {datasource}")
            return accounts_to_create
        else:
            self.logger.log(f"No {account_type} accounts need to be created for {customer_type} from {datasource}")
            return 0
    
    def _get_future_date(self, days: int = 1) -> str:
        """
        Get future date in MM/DD/YYYY format.
        
        Args:
            days: Number of days in the future
            
        Returns:
            Formatted date string
        """
        future_date = date.today() + timedelta(days=days)
        return future_date.strftime("%m/%d/%Y")



# app/account_automation/jobs/custom_threshold_job.py
"""
Job implementation for custom threshold account creation.
"""
from typing import Dict, Any, List, Optional
from app.account_automation.jobs.base_job import BaseJob


class CustomThresholdJob(BaseJob):
    """
    Job for creating accounts with a user-defined threshold.
    
    This job allows specifying a custom threshold instead of using
    the threshold from configuration.
    """
    
    def __init__(self, account_service, customer_service, inquiry_service, 
                query_repository, config=None, email_service=None, job_control_service=None,
                account_type="dda", customer_type="P", data_source="Mining"):
        """
        Initialize Custom Threshold job.
        
        Args:
            account_service: Account service for account operations
            customer_service: Customer service for customer operations
            inquiry_service: Inquiry service for inquiries
            query_repository: Repository for SQL queries
            config: Optional configuration instance
            email_service: Optional email service for notifications
            job_control_service: Optional job control service
            account_type: Type of account (dda, cca)
            customer_type: Type of customer (P/B)
            data_source: Data source (Mining/SDG)
        """
        job_name = f"custom_{account_type}_{customer_type.lower()}_{data_source.lower()}"
        super().__init__(job_name, account_service.logger, config, email_service)
        
        # Store dependencies
        self.account_service = account_service
        self.customer_service = customer_service
        self.inquiry_service = inquiry_service
        self.query_repository = query_repository
        self.job_control_service = job_control_service
        
        # Store account parameters
        self.account_type = account_type
        self.customer_type = customer_type
        self.data_source = data_source
    
    def _execute(self, region: str = "SIT1", threshold: int = 1000, **kwargs) -> Dict[str, Any]:
        """
        Execute the custom threshold job.
        
        Args:
            region: Target region
            threshold: User-defined threshold
            **kwargs: Additional parameters
            
        Returns:
            Dict containing job execution results
        """
        # Check if job is enabled
        if self.job_control_service and not self.job_control_service.is_job_enabled(self.name, region):
            self.logger.log(f"Job {self.name} is disabled for region {region}")
            return {
                "status": "skipped",
                "message": f"Job {self.name} is disabled for region {region}",
                "created_count": 0,
                "region": region
            }
        
        self.logger.log(f"Creating {self.account_type} {self.customer_type} accounts up to custom threshold {threshold} in {region}")
        
        # Create accounts based on account type, customer type, and data source
        created_accounts = []
        success_count = 0
        failure_count = 0
        
        for i in range(threshold):
            try:
                self.logger.log(f"Creating {self.account_type} {self.customer_type} account {i+1}/{threshold} from {self.data_source}")
                
                account_number = None
                
                # Create account based on type
                if self.account_type == "dda":
                    if self.data_source == "Mining":
                        account_number = self.account_service.create_dda_mining(self.customer_type, region)
                    else:  # SDG
                        result = self.account_service.create_dda_synthetic(self.customer_type, region)
                        account_number = result["DepositAccounts"]["Accounts"][0]["AccountNumber"]
                
                elif self.account_type == "cca":
                    if self.data_source == "Mining":
                        account_number = self.account_service.create_cca_mining("", region)
                    else:  # SDG
                        result = self.account_service.create_cca_synthetic(self.customer_type, region)
                        account_number = result["Account"]["AccountNumber"]
                
                # Record created account
                if account_number:
                    created_accounts.append({
                        "account_number": account_number,
                        "account_type": self.account_type,
                        "customer_type": self.customer_type,
                        "data_source": self.data_source
                    })
                    
                    success_count += 1
                    self.logger.log(f"Successfully created account: {account_number}")
                
            except Exception as e:
                self.logger.error(f"Failed to create account: {str(e)}")
                failure_count += 1
        
        # Update job summary
        self.summary["success_count"] = success_count
        self.summary["failure_count"] = failure_count
        
        # Return detailed results
        return {
            "custom_threshold": threshold,
            "created_count": success_count,
            "failed_count": failure_count,
            "region": region,
            "account_type": self.account_type,
            "customer_type": self.customer_type,
            "data_source": self.data_source,
            "created_accounts": created_accounts
        }


class CustomDDAPersonalMiningJob(CustomThresholdJob):
    """Job for creating personal DDA accounts from mining source with custom threshold."""
    
    def __init__(self, account_service, customer_service, inquiry_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize Custom DDA Personal Mining job."""
        super().__init__(
            account_service=account_service,
            customer_service=customer_service,
            inquiry_service=inquiry_service,
            query_repository=query_repository,
            config=config,
            email_service=email_service,
            job_control_service=job_control_service,
            account_type="dda",
            customer_type="P",
            data_source="Mining"
        )


class CustomDDAPersonalSDGJob(CustomThresholdJob):
    """Job for creating personal DDA accounts from SDG source with custom threshold."""
    
    def __init__(self, account_service, customer_service, inquiry_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize Custom DDA Personal SDG job."""
        super().__init__(
            account_service=account_service,
            customer_service=customer_service,
            inquiry_service=inquiry_service,
            query_repository=query_repository,
            config=config,
            email_service=email_service,
            job_control_service=job_control_service,
            account_type="dda",
            customer_type="P",
            data_source="SDG"
        )


class CustomDDABusinessMiningJob(CustomThresholdJob):
    """Job for creating business DDA accounts from mining source with custom threshold."""
    
    def __init__(self, account_service, customer_service, inquiry_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize Custom DDA Business Mining job."""
        super().__init__(
            account_service=account_service,
            customer_service=customer_service,
            inquiry_service=inquiry_service,
            query_repository=query_repository,
            config=config,
            email_service=email_service,
            job_control_service=job_control_service,
            account_type="dda",
            customer_type="B",
            data_source="Mining"
        )


class CustomDDABusinessSDGJob(CustomThresholdJob):
    """Job for creating business DDA accounts from SDG source with custom threshold."""
    
    def __init__(self, account_service, customer_service, inquiry_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize Custom DDA Business SDG job."""
        super().__init__(
            account_service=account_service,
            customer_service=customer_service,
            inquiry_service=inquiry_service,
            query_repository=query_repository,
            config=config,
            email_service=email_service,
            job_control_service=job_control_service,
            account_type="dda",
            customer_type="B",
            data_source="SDG"
        )


class CustomCCAPersonalMiningJob(CustomThresholdJob):
    """Job for creating personal CCA accounts from mining source with custom threshold."""
    
    def __init__(self, account_service, customer_service, inquiry_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize Custom CCA Personal Mining job."""
        super().__init__(
            account_service=account_service,
            customer_service=customer_service,
            inquiry_service=inquiry_service,
            query_repository=query_repository,
            config=config,
            email_service=email_service,
            job_control_service=job_control_service,
            account_type="cca",
            customer_type="P",
            data_source="Mining"
        )


class CustomCCABusinessMiningJob(CustomThresholdJob):
    """Job for creating business CCA accounts from mining source with custom threshold."""
    
    def __init__(self, account_service, customer_service, inquiry_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize Custom CCA Business Mining job."""
        super().__init__(
            account_service=account_service,
            customer_service=customer_service,
            inquiry_service=inquiry_service,
            query_repository=query_repository,
            config=config,
            email_service=email_service,
            job_control_service=job_control_service,
            account_type="cca",
            customer_type="B",
            data_source="Mining"
        )


class CustomCCAPersonalSDGJob(CustomThresholdJob):
    """Job for creating personal CCA accounts from SDG source with custom threshold."""
    
    def __init__(self, account_service, customer_service, inquiry_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize Custom CCA Personal SDG job."""
        super().__init__(
            account_service=account_service,
            customer_service=customer_service,
            inquiry_service=inquiry_service,
            query_repository=query_repository,
            config=config,
            email_service=email_service,
            job_control_service=job_control_service,
            account_type="cca",
            customer_type="P",
            data_source="SDG"
        )


class CustomCCABusinessSDGJob(CustomThresholdJob):
    """Job for creating business CCA accounts from SDG source with custom threshold."""
    
    def __init__(self, account_service, customer_service, inquiry_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize Custom CCA Business SDG job."""
        super().__init__(
            account_service=account_service,
            customer_service=customer_service,
            inquiry_service=inquiry_service,
            query_repository=query_repository,
            config=config,
            email_service=email_service,
            job_control_service=job_control_service,
            account_type="cca",
            customer_type="B",
            data_source="SDG"
        )



#app/account_automation/jobs/job_factory.py
"""
Factory for creating job instances based on job type.
"""
from typing import Dict, Any, Optional

from app.account_automation.jobs.dda_job import (
    DDAMiningPersonalJob, DDAMiningBusinessJob,
    DDASDGPersonalJob, DDASDGBusinessJob,
    DDAThresholdPersonalJob, DDAThresholdBusinessJob
)
from app.account_automation.jobs.cca_job import (
    CCAMiningJob, CCASDGPersonalJob, CCASDGBusinessJob,
    CCAThresholdPersonalJob, CCAThresholdBusinessJob
)
from app.account_automation.jobs.im80_job import IM80Job
from app.account_automation.jobs.custom_threshold_job import (
    CustomDDAPersonalMiningJob, CustomDDAPersonalSDGJob,
    CustomDDABusinessMiningJob, CustomDDABusinessSDGJob,
    CustomCCAPersonalMiningJob, CustomCCAPersonalSDGJob,
    CustomCCABusinessMiningJob, CustomCCABusinessSDGJob
)


class JobFactory:
    """
    Factory for creating job instances based on job type.
    
    This factory creates appropriate job instances based on the requested job type,
    injecting the necessary dependencies.
    """
    
    def __init__(self, container):
        """
        Initialize the job factory with dependency container.
        
        Args:
            container: Dependency injection container for resolving dependencies
        """
        self.container = container
    
    def get_job(self, job_type: str) -> Optional[Any]:
        """
        Get a job instance based on job type.
        
        Args:
            job_type: Type of job to create
            
        Returns:
            Job instance or None if job type is not supported
        """
        # Get common dependencies
        logger = self.container.get('logger')
        config = self.container.get('config')
        email_service = self.container.get('email_service')
        job_control_service = self.container.get('job_control_service')
        query_repository = self.container.get('query_repository')
        
        # Service dependencies
        account_service = self.container.get('account_service')
        customer_service = self.container.get('customer_service')
        inquiry_service = self.container.get('inquiry_service')
        transaction_service = self.container.get('transaction_service')
        
        # DDA Jobs - Personal
        if job_type == "dda_threshold_p":
            return DDAThresholdPersonalJob(
                account_service=account_service,
                customer_service=customer_service,
                inquiry_service=inquiry_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        elif job_type == "dda_mining_p":
            return DDAMiningPersonalJob(
                account_service=account_service,
                customer_service=customer_service,
                inquiry_service=inquiry_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        elif job_type == "dda_sdg_p":
            return DDASDGPersonalJob(
                account_service=account_service,
                customer_service=customer_service,
                inquiry_service=inquiry_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        # DDA Jobs - Business
        elif job_type == "dda_threshold_b":
            return DDAThresholdBusinessJob(
                account_service=account_service,
                customer_service=customer_service,
                inquiry_service=inquiry_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        elif job_type == "dda_mining_b":
            return DDAMiningBusinessJob(
                account_service=account_service,
                customer_service=customer_service,
                inquiry_service=inquiry_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        elif job_type == "dda_sdg_b":
            return DDASDGBusinessJob(
                account_service=account_service,
                customer_service=customer_service,
                inquiry_service=inquiry_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        # Custom Threshold DDA Jobs
        elif job_type == "custom_dda_p_mining":
            return CustomDDAPersonalMiningJob(
                account_service=account_service,
                customer_service=customer_service,
                inquiry_service=inquiry_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        elif job_type == "custom_dda_p_sdg":
            return CustomDDAPersonalSDGJob(
                account_service=account_service,
                customer_service=customer_service,
                inquiry_service=inquiry_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        elif job_type == "custom_dda_b_mining":
            return CustomDDABusinessMiningJob(
                account_service=account_service,
                customer_service=customer_service,
                inquiry_service=inquiry_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        elif job_type == "custom_dda_b_sdg":
            return CustomDDABusinessSDGJob(
                account_service=account_service,
                customer_service=customer_service,
                inquiry_service=inquiry_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        # CCA Jobs - Personal
        elif job_type == "cca_threshold_p":
            return CCAThresholdPersonalJob(
                account_service=account_service,
                customer_service=customer_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        elif job_type == "cca_sdg_p":
            return CCASDGPersonalJob(
                account_service=account_service,
                customer_service=customer_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        # CCA Jobs - Business
        elif job_type == "cca_threshold_b":
            return CCAThresholdBusinessJob(
                account_service=account_service,
                customer_service=customer_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        elif job_type == "cca_sdg_b":
            return CCASDGBusinessJob(
                account_service=account_service,
                customer_service=customer_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        # Custom Threshold CCA Jobs
        elif job_type == "custom_cca_p_mining":
            return CustomCCAPersonalMiningJob(
                account_service=account_service,
                customer_service=customer_service,
                inquiry_service=inquiry_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        elif job_type == "custom_cca_p_sdg":
            return CustomCCAPersonalSDGJob(
                account_service=account_service,
                customer_service=customer_service,
                inquiry_service=inquiry_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        elif job_type == "custom_cca_b_mining":
            return CustomCCABusinessMiningJob(
                account_service=account_service,
                customer_service=customer_service,
                inquiry_service=inquiry_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        elif job_type == "custom_cca_b_sdg":
            return CustomCCABusinessSDGJob(
                account_service=account_service,
                customer_service=customer_service,
                inquiry_service=inquiry_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        # CCA Jobs - Mining (common for both personal and business)
        elif job_type == "cca_mining":
            return CCAMiningJob(
                account_service=account_service,
                customer_service=customer_service,
                query_repository=query_repository,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        # IM80 Transaction Jobs
        elif job_type == "im80":
            return IM80Job(
                transaction_service=transaction_service,
                query_repository=query_repository,
                logger=logger,
                config=config,
                email_service=email_service,
                job_control_service=job_control_service
            )
        
        # Unknown job type
        else:
            logger.error(f"Unknown job type: {job_type}")
            return None



# app/account_automation/controllers/account_controller.py
"""
Controller for account-related API endpoints.
"""
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Body
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field


# Request and response models
class AccountRequest(BaseModel):
    """Model for account creation request."""
    customer_type: str = Field(..., description="Customer type (P/B)")
    region: str = Field("SIT1", description="Target region")
    product_type: Optional[str] = Field(None, description="Specific product type")


class AccountResponse(BaseModel):
    """Model for account creation response."""
    status: str = Field(..., description="Status of the operation (success/error)")
    message: Optional[str] = Field(None, description="Response message")
    account_number: Optional[str] = Field(None, description="Created account number")
    details: Optional[Dict[str, Any]] = Field(None, description="Account details")


class MiningAccountRequest(BaseModel):
    """Model for mining account creation request."""
    customer_type: str = Field(..., description="Customer type (P/B)")
    region: str = Field("SIT1", description="Target region")


class ThresholdResponse(BaseModel):
    """Model for threshold response."""
    status: str = Field(..., description="Status of the operation (success/error)")
    message: str = Field(..., description="Response message")
    region: str = Field(..., description="Target region")
    thresholds: Dict[str, Dict[str, Any]] = Field(..., description="Account creation thresholds")


class BulkCreateRequest(BaseModel):
    """Model for bulk account creation request."""
    job_type: str = Field(..., description="Job type to execute")
    region: str = Field("SIT1", description="Target region")
    count: Optional[int] = Field(None, description="Number of accounts to create")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Additional parameters")


class CustomThresholdRequest(BaseModel):
    """Model for custom threshold account creation request."""
    account_type: str = Field(..., description="Account type (dda/cca)")
    customer_type: str = Field(..., description="Customer type (P/B)")
    data_source: str = Field(..., description="Data source (Mining/SDG)")
    region: str = Field("SIT1", description="Target region")
    threshold: int = Field(..., description="Custom threshold (number of accounts to create)")


class AccountController:
    """
    Controller for account-related API endpoints.
    
    This controller handles the HTTP endpoints for creating and managing
    DDA, DCA, and CCA accounts.
    """
    
    def __init__(self, container):
        """
        Initialize account controller with DI container.
        
        Args:
            container: Dependency injection container
        """
        self.container = container
        self.account_service = container.get('account_service')
        self.job_factory = container.get('job_factory')
        self.logger = container.get('logger')
        
        # Create router with prefix and tags
        self.router = APIRouter(prefix="/api/accounts", tags=["Accounts"])
        
        # Register routes
        self._register_routes()
    
    def _register_routes(self):
        """Register API routes for account operations."""
        # DDA routes
        self.router.add_api_route("/dda", self.create_dda, methods=["POST"], response_model=AccountResponse)
        self.router.add_api_route("/dda/mining", self.create_dda_mining, methods=["POST"], response_model=AccountResponse)
        
        # CCA routes
        self.router.add_api_route("/cca", self.create_cca, methods=["POST"], response_model=AccountResponse)
        self.router.add_api_route("/cca/mining", self.create_cca_mining, methods=["POST"], response_model=AccountResponse)
        
        # Common routes
        self.router.add_api_route("/thresholds", self.get_thresholds, methods=["GET"], response_model=ThresholdResponse)
        self.router.add_api_route("/bulk", self.create_bulk, methods=["POST"], response_model=Dict[str, Any])
        
        # Custom threshold route
        self.router.add_api_route("/custom-threshold", self.create_with_custom_threshold, methods=["POST"], response_model=Dict[str, Any])
    
    async def create_dda(self, request: AccountRequest) -> AccountResponse:
        """
        Create a synthetic DDA (Demand Deposit Account).
        
        Args:
            request: Account creation request parameters
            
        Returns:
            AccountResponse: Account creation result
        """
        try:
            self.logger.log(f"API request to create synthetic DDA {request.customer_type} account in {request.region}")
            
            # Create account using account service
            account_data = self.account_service.create_dda_synthetic(
                customer_type=request.customer_type,
                region=request.region,
                product_type_alpha=request.product_type
            )
            
            # Extract account number from response
            account_number = account_data["DepositAccounts"]["Accounts"][0]["AccountNumber"]
            
            # Return success response
            return AccountResponse(
                status="success",
                message=f"DDA {request.customer_type} account created successfully",
                account_number=account_number,
                details=account_data
            )
            
        except Exception as e:
            # Log error and return error response
            self.logger.error(f"Error creating synthetic DDA account: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
    
    async def create_dda_mining(self, request: MiningAccountRequest) -> AccountResponse:
        """
        Create a DDA (Demand Deposit Account) from mining source.
        
        Args:
            request: Mining account creation request parameters
            
        Returns:
            AccountResponse: Account creation result
        """
        try:
            self.logger.log(f"API request to create mining DDA {request.customer_type} account in {request.region}")
            
            # Create account using account service
            account_number = self.account_service.create_dda_mining(
                customer_type=request.customer_type,
                region=request.region
            )
            
            # Return success response
            return AccountResponse(
                status="success",
                message=f"Mining DDA {request.customer_type} account created successfully",
                account_number=account_number
            )
            
        except Exception as e:
            # Log error and return error response
            self.logger.error(f"Error creating mining DDA account: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
    
    async def create_cca(self, request: AccountRequest) -> AccountResponse:
        """
        Create a synthetic CCA (Credit Card Account).
        
        Args:
            request: Account creation request parameters
            
        Returns:
            AccountResponse: Account creation result
        """
        try:
            self.logger.log(f"API request to create synthetic CCA {request.customer_type} account in {request.region}")
            
            # Create account using account service
            account_data = self.account_service.create_cca_synthetic(
                customer_type=request.customer_type,
                region=request.region,
                product_type_alpha=request.product_type
            )
            
            # Extract account number from response
            account_number = account_data["Account"]["AccountNumber"]
            
            # Return success response
            return AccountResponse(
                status="success",
                message=f"CCA {request.customer_type} account created successfully",
                account_number=account_number,
                details=account_data
            )
            
        except Exception as e:
            # Log error and return error response
            self.logger.error(f"Error creating synthetic CCA account: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
    
    async def create_cca_mining(self, request: MiningAccountRequest) -> AccountResponse:
        """
        Create a CCA (Credit Card Account) from mining source.
        
        Args:
            request: Mining account creation request parameters
            
        Returns:
            AccountResponse: Account creation result
        """
        try:
            self.logger.log(f"API request to create mining CCA {request.customer_type} account in {request.region}")
            
            # Create account using account service
            account_number = self.account_service.create_cca_mining(
                party_id="",  # Empty party ID to use mining source
                region=request.region
            )
            
            # Return success response
            return AccountResponse(
                status="success",
                message=f"Mining CCA {request.customer_type} account created successfully",
                account_number=account_number
            )
            
        except Exception as e:
            # Log error and return error response
            self.logger.error(f"Error creating mining CCA account: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
    
    async def get_thresholds(self, region: str = Query("SIT1", description="Target region")) -> ThresholdResponse:
        """
        Get account creation thresholds for all account types.
        
        Args:
            region: Target region
            
        Returns:
            ThresholdResponse: Account creation thresholds
        """
        try:
            self.logger.log(f"API request to get account thresholds for {region}")
            
            # Get thresholds for all account types and sources
            thresholds = {
                "dda": {
                    "personal": {
                        "mining": self.account_service.get_threshold("dda", "P", region, "Mining"),
                        "sdg": self.account_service.get_threshold("dda", "P", region, "SDG")
                    },
                    "business": {
                        "mining": self.account_service.get_threshold("dda", "B", region, "Mining"),
                        "sdg": self.account_service.get_threshold("dda", "B", region, "SDG")
                    }
                },
                "cca": {
                    "personal": {
                        "mining": self.account_service.get_threshold("cca", "P", region, "Mining"),
                        "sdg": self.account_service.get_threshold("cca", "P", region, "SDG")
                    },
                    "business": {
                        "mining": self.account_service.get_threshold("cca", "B", region, "Mining"),
                        "sdg": self.account_service.get_threshold("cca", "B", region, "SDG")
                    }
                }
            }
            
            # Return response with thresholds
            return ThresholdResponse(
                status="success",
                message="Account thresholds retrieved successfully",
                region=region,
                thresholds=thresholds
            )
            
        except Exception as e:
            # Log error and return error response
            self.logger.error(f"Error getting account thresholds: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
    
    async def create_bulk(self, request: BulkCreateRequest) -> Dict[str, Any]:
        """
        Create accounts in bulk using a job.
        
        Args:
            request: Bulk account creation request
            
        Returns:
            Dict: Bulk creation results
        """
        try:
            self.logger.log(f"API request to create accounts in bulk with job {request.job_type}")
            
            # Get job from factory
            job = self.job_factory.get_job(request.job_type)
            
            if not job:
                raise Exception(f"Unknown job type: {request.job_type}")
            
            # Prepare parameters
            params = request.parameters.copy()
            params["region"] = request.region
            
            if request.count is not None:
                params["count"] = request.count
                
            # Execute job with parameters
            result = job.execute(**params)
            
            # Return job execution result
            return {
                "status": "success",
                "message": f"Created {result.get('created_count', 0)} accounts successfully",
                "job_type": request.job_type,
                "result": result
            }
            
        except Exception as e:
            # Log error and return error response
            self.logger.error(f"Error creating accounts in bulk: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
    
    async def create_with_custom_threshold(self, request: CustomThresholdRequest) -> Dict[str, Any]:
        """
        Create accounts with a custom threshold.
        
        Args:
            request: Custom threshold request
            
        Returns:
            Dict: Creation results
        """
        try:
            self.logger.log(f"API request to create {request.account_type} {request.customer_type} accounts with custom threshold {request.threshold}")
            
            # Determine job type
            job_type = f"custom_{request.account_type}_{request.customer_type.lower()}_{request.data_source.lower()}"
            
            # Get job from factory
            job = self.job_factory.get_job(job_type)
            
            if not job:
                raise Exception(f"Unknown job type: {job_type}")
            
            # Execute job with parameters
            result = job.execute(
                region=request.region,
                threshold=request.threshold
            )
            
            # Return job execution result
            return {
                "status": "success",
                "message": f"Created {result.get('created_count', 0)} accounts successfully",
                "job_type": job_type,
                "custom_threshold": request.threshold,
                "result": result
            }
            
        except Exception as e:
            # Log error and return error response
            self.logger.error(f"Error creating accounts with custom threshold: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))


# app/account_automation/scheduler_setup.py
"""
Scheduler setup for account automation jobs.

This module provides functions to set up scheduled jobs for account automation.
"""
import os
from typing import Dict, Any, List, Optional


def setup_scheduler(container) -> None:
    """
    Set up the scheduler with predefined jobs.
    
    Args:
        container: Dependency injection container
    """
    # Get scheduler client and logger
    scheduler_client = container.get('scheduler_client')
    logger = container.get('logger')
    db = container.get('db_auto')
    query_repository = container.get('query_repository')
    
    logger.log("Setting up scheduler with predefined jobs")
    
    try:
        # Get regions from database
        regions = _get_regions(db, query_repository, logger)
        
        if not regions:
            logger.warning("No regions found. Using default region SIT1")
            regions = ["SIT1"]
        
        # Schedule predefined jobs for each region
        _schedule_predefined_jobs(scheduler_client, logger, regions)
        
        # Start scheduler if enabled
        if os.getenv('ENABLE_SCHEDULER', 'False').lower() == 'true':
            scheduler_client.start()
            logger.log("Scheduler started")
        else:
            logger.log("Scheduler not started (ENABLE_SCHEDULER is not set to 'true')")
    
    except Exception as e:
        logger.error(f"Error setting up scheduler: {str(e)}")
        
        # Try to schedule default jobs
        try:
            _schedule_default_jobs(scheduler_client, logger)
            
            # Start scheduler if enabled
            if os.getenv('ENABLE_SCHEDULER', 'False').lower() == 'true':
                scheduler_client.start()
                logger.log("Scheduler started with default jobs")
        except Exception as inner_e:
            logger.error(f"Error scheduling default jobs: {str(inner_e)}")


def _get_regions(db, query_repository, logger) -> List[str]:
    """
    Get list of regions from database.
    
    Args:
        db: Database connection
        query_repository: Repository for SQL queries
        logger: Logger instance
        
    Returns:
        List[str]: List of region codes
    """
    try:
        # Get query for regions
        query = query_repository.get_query('get_regions')
        
        # Execute stored procedure to get regions
        result = db.execute(query)
        
        # Extract region codes
        regions = []
        for row in result:
            if hasattr(row, 'TestLevel'):
                regions.append(row.TestLevel)
        
        logger.log(f"Found regions: {', '.join(regions)}")
        return regions
    
    except Exception as e:
        logger.error(f"Error getting regions: {str(e)}")
        return []


def _schedule_predefined_jobs(scheduler_client, logger, regions: List[str]) -> None:
    """
    Schedule predefined jobs for each region.
    
    Args:
        scheduler_client: Scheduler client
        logger: Logger instance
        regions: List of region codes
    """
    for region in regions:
        logger.log(f"Scheduling jobs for region {region}")
        
        # DDA threshold jobs
        scheduler_client.schedule_job(
            job_type="dda_threshold_p",
            interval_minutes=24 * 60,  # Daily
            params={
                "region": region,
                "threshold": 100
            },
            enabled=True
        )
        
        scheduler_client.schedule_job(
            job_type="dda_threshold_b",
            interval_minutes=24 * 60,  # Daily
            params={
                "region": region,
                "threshold": 50
            },
            enabled=True
        )
        
        # CCA threshold jobs
        scheduler_client.schedule_job(
            job_type="cca_threshold_p",
            interval_minutes=24 * 60,  # Daily
            params={
                "region": region,
                "threshold": 50
            },
            enabled=True
        )
        
        scheduler_client.schedule_job(
            job_type="cca_threshold_b",
            interval_minutes=24 * 60,  # Daily
            params={
                "region": region,
                "threshold": 30
            },
            enabled=True
        )
        
        # IM80 transaction job
        scheduler_client.schedule_job(
            job_type="im80",
            interval_minutes=4 * 60,  # Every 4 hours
            params={
                "region": region,
                "account_type": "P",
                "max_transactions": 20,
                "amount": "3000.00"
            },
            enabled=True
        )
        
        # IM80 transaction job for business accounts
        scheduler_client.schedule_job(
            job_type="im80",
            interval_minutes=4 * 60,  # Every 4 hours
            params={
                "region": region,
                "account_type": "B",
                "max_transactions": 10,
                "amount": "5000.00"
            },
            enabled=True
        )
        
        # Schedule custom threshold jobs for demonstration
        scheduler_client.schedule_job(
            job_type="custom_dda_p_mining",
            interval_minutes=24 * 60,  # Daily
            params={
                "region": region,
                "threshold": 1000  # User-defined threshold
            },
            enabled=False  # Disabled by default, to be enabled when needed
        )


def _schedule_default_jobs(scheduler_client, logger) -> None:
    """
    Schedule default jobs for SIT1 region.
    
    This is a fallback if getting regions from the database fails.
    
    Args:
        scheduler_client: Scheduler client
        logger: Logger instance
    """
    region = "SIT1"
    logger.log(f"Scheduling default jobs for region {region}")
    
    # DDA threshold jobs
    scheduler_client.schedule_job(
        job_type="dda_threshold_p",
        interval_minutes=24 * 60,  # Daily
        params={
            "region": region,
            "threshold": 100
        },
        enabled=True
    )
    
    # CCA threshold jobs
    scheduler_client.schedule_job(
        job_type="cca_threshold_p",
        interval_minutes=24 * 60,  # Daily
        params={
            "region": region,
            "threshold": 50
        },
        enabled=True
    )
    
    # IM80 transaction job
    scheduler_client.schedule_job(
        job_type="im80",
        interval_minutes=4 * 60,  # Every 4 hours
        params={
            "region": region,
            "account_type": "P",
            "max_transactions": 20,
            "amount": "3000.00"
        },
        enabled=True
    )



# app/account_automation/jobs/dda_job.py
"""
Job implementations for DDA account creation operations.
"""
import random
import json
from typing import Dict, Any, List, Optional
from app.account_automation.jobs.base_job import BaseJob


class DDAJob(BaseJob):
    """
    Base class for DDA (Demand Deposit Account) creation jobs.
    
    This class provides common functionality for DDA account creation
    jobs, regardless of the data source or customer type.
    """
    
    def __init__(self, name, account_service, customer_service, 
                inquiry_service, query_repository, config=None, email_service=None, 
                job_control_service=None, customer_type="P", data_source="Mining"):
        """
        Initialize DDA job base class.
        
        Args:
            name: Job name
            account_service: Account service for account operations
            customer_service: Customer service for customer operations
            inquiry_service: Inquiry service for inquiries
            query_repository: Repository for SQL queries
            config: Optional configuration instance
            email_service: Optional email service for notifications
            job_control_service: Optional job control service
            customer_type: Customer type (P for Personal, B for Business)
            data_source: Data source (Mining or SDG)
        """
        super().__init__(name, account_service.logger, config, email_service)
        
        # Store dependencies
        self.account_service = account_service
        self.customer_service = customer_service
        self.inquiry_service = inquiry_service
        self.query_repository = query_repository
        self.job_control_service = job_control_service
        
        # Store customer type and data source
        self.customer_type = customer_type
        self.data_source = data_source
        
        # Load signal data from database if available
        self._load_signal_data()
    
    def _load_signal_data(self) -> None:
        """
        Load signal data from database.
        
        This method loads configuration from the AutoSignalData table.
        """
        try:
            db = self.account_service.db
            query = self.query_repository.get_query('get_signal_data')
            
            # Determine signal data key based on region
            signal_key = 'RES Create DDA'
            
            signal_data = db.query_single(db.execute_param(query, [signal_key, 'RES']))
            
            if signal_data and hasattr(signal_data, 'SValue'):
                try:
                    # Parse JSON data
                    self.signal_data = json.loads(signal_data.SValue)
                    
                    # Log successful load
                    self.logger.log(f"Loaded signal data for {signal_key}")
                except json.JSONDecodeError as e:
                    self.logger.error(f"Error parsing signal data: {str(e)}")
                    self.signal_data = {}
            else:
                self.logger.warning(f"No signal data found for {signal_key}")
                self.signal_data = {}
        except Exception as e:
            self.logger.error(f"Error loading signal data: {str(e)}")
            self.signal_data = {}
    
    def get_threshold(self, region: str) -> int:
        """
        Get threshold for account creation based on configuration.
        
        Args:
            region: Target region
            
        Returns:
            Number of accounts to create
        """
        # Use account service to determine threshold
        return self.account_service.get_threshold(
            account_type="dda",
            customer_type=self.customer_type,
            region=region,
            datasource=self.data_source
        )
    
    def _execute(self, region: str = "SIT1", count: int = None, **kwargs) -> Dict[str, Any]:
        """
        Execute the DDA job.
        
        Args:
            region: Target region
            count: Number of accounts to create (overrides threshold if provided)
            **kwargs: Additional parameters
            
        Returns:
            Dict containing job execution results
        """
        # Check if job is enabled via job control service
        if self.job_control_service and not self.job_control_service.is_job_enabled(self.name, region):
            self.logger.log(f"Job {self.name} is disabled for region {region}")
            return {
                "status": "skipped",
                "message": f"Job {self.name} is disabled for region {region}",
                "created_count": 0,
                "region": region
            }
        
        # Determine number of accounts to create
        accounts_to_create = count if count is not None else self.get_threshold(region)
        
        self.logger.log(f"Creating {accounts_to_create} {self.customer_type} DDA accounts from {self.data_source} in region {region}")
        
        # No accounts to create
        if accounts_to_create <= 0:
            self.logger.log(f"No {self.customer_type} DDA accounts need to be created from {self.data_source} in {region}")
            return {
                "created_count": 0,
                "failed_count": 0,
                "region": region,
                "customer_type": self.customer_type,
                "data_source": self.data_source,
                "created_accounts": []
            }
        
        # Create accounts
        created_accounts = []
        success_count = 0
        failure_count = 0
        
        for i in range(accounts_to_create):
            try:
                self.logger.log(f"Creating {self.customer_type} DDA account {i+1}/{accounts_to_create} from {self.data_source}")
                
                # Create the account based on data source
                if self.data_source == "Mining":
                    account_number = self.account_service.create_dda_mining(self.customer_type, region)
                    created_accounts.append({
                        "account_number": account_number,
                        "source": "Mining",
                        "type": "Personal" if self.customer_type == "P" else "Business"
                    })
                else:  # SDG
                    result = self.account_service.create_dda_synthetic(self.customer_type, region)
                    account_number = result["DepositAccounts"]["Accounts"][0]["AccountNumber"]
                    created_accounts.append({
                        "account_number": account_number,
                        "source": "SDG",
                        "type": "Personal" if self.customer_type == "P" else "Business",
                        "product_type_alpha": result["DepositAccounts"]["Accounts"][0]["ProductTypeAlpha"]
                    })
                
                self.logger.log(f"Successfully created DDA account: {account_number}")
                success_count += 1
                
            except Exception as e:
                self.logger.error(f"Failed to create {self.customer_type} DDA account from {self.data_source}: {str(e)}")
                failure_count += 1
        
        # Update job summary
        self.summary["success_count"] = success_count
        self.summary["failure_count"] = failure_count
        
        # Return detailed results
        return {
            "count": accounts_to_create,
            "created_count": success_count,
            "failed_count": failure_count,
            "region": region,
            "customer_type": self.customer_type,
            "data_source": self.data_source,
            "created_accounts": created_accounts
        }


class DDAMiningPersonalJob(DDAJob):
    """Job for creating personal DDA accounts from mining source."""
    
    def __init__(self, account_service, customer_service, inquiry_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize DDA Mining Personal job."""
        super().__init__(
            name="dda_mining_p",
            account_service=account_service,
            customer_service=customer_service,
            inquiry_service=inquiry_service,
            query_repository=query_repository,
            config=config,
            email_service=email_service,
            job_control_service=job_control_service,
            customer_type="P",
            data_source="Mining"
        )


class DDAMiningBusinessJob(DDAJob):
    """Job for creating business DDA accounts from mining source."""
    
    def __init__(self, account_service, customer_service, inquiry_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize DDA Mining Business job."""
        super().__init__(
            name="dda_mining_b",
            account_service=account_service,
            customer_service=customer_service,
            inquiry_service=inquiry_service,
            query_repository=query_repository,
            config=config,
            email_service=email_service,
            job_control_service=job_control_service,
            customer_type="B",
            data_source="Mining"
        )


class DDASDGPersonalJob(DDAJob):
    """Job for creating personal DDA accounts from SDG source."""
    
    def __init__(self, account_service, customer_service, inquiry_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize DDA SDG Personal job."""
        super().__init__(
            name="dda_sdg_p",
            account_service=account_service,
            customer_service=customer_service,
            inquiry_service=inquiry_service,
            query_repository=query_repository,
            config=config,
            email_service=email_service,
            job_control_service=job_control_service,
            customer_type="P",
            data_source="SDG"
        )


class DDASDGBusinessJob(DDAJob):
    """Job for creating business DDA accounts from SDG source."""
    
    def __init__(self, account_service, customer_service, inquiry_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize DDA SDG Business job."""
        super().__init__(
            name="dda_sdg_b",
            account_service=account_service,
            customer_service=customer_service,
            inquiry_service=inquiry_service,
            query_repository=query_repository,
            config=config,
            email_service=email_service,
            job_control_service=job_control_service,
            customer_type="B",
            data_source="SDG"
        )


class DDAThresholdPersonalJob(BaseJob):
    """
    Job for creating personal DDA accounts up to a configured threshold.
    
    This job creates a mix of personal accounts from both mining and SDG sources
    up to a specified threshold.
    """
    
    def __init__(self, account_service, customer_service, inquiry_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize DDA Threshold Personal job."""
        super().__init__("dda_threshold_p", account_service.logger, config, email_service)
        
        # Store dependencies
        self.account_service = account_service
        self.customer_service = customer_service
        self.inquiry_service = inquiry_service
        self.query_repository = query_repository
        self.job_control_service = job_control_service
    
    def _execute(self, region: str = "SIT1", threshold: int = 100, mining_percent: int = 50, **kwargs) -> Dict[str, Any]:
        """
        Execute the DDA threshold job for personal accounts.
        
        Args:
            region: Target region
            threshold: Maximum number of accounts to create
            mining_percent: Percentage of accounts to create from mining source (0-100)
            **kwargs: Additional parameters
            
        Returns:
            Dict containing job execution results
        """
        # Check if job is enabled
        if self.job_control_service and not self.job_control_service.is_job_enabled(self.name, region):
            self.logger.log(f"Job {self.name} is disabled for region {region}")
            return {
                "status": "skipped",
                "message": f"Job {self.name} is disabled for region {region}",
                "created_count": 0,
                "region": region
            }
        
        self.logger.log(f"Creating personal DDA accounts up to threshold {threshold} in region {region}")
        
        # Calculate counts for each source
        mining_count = int(threshold * mining_percent / 100)
        sdg_count = threshold - mining_count
        
        # Create mining accounts
        mining_job = DDAMiningPersonalJob(
            account_service=self.account_service,
            customer_service=self.customer_service,
            inquiry_service=self.inquiry_service,
            query_repository=self.query_repository,
            config=self.config,
            email_service=self.email_service,
            job_control_service=self.job_control_service
        )
        
        mining_result = mining_job.execute(region=region, count=mining_count)
        
        # Create SDG accounts
        sdg_job = DDASDGPersonalJob(
            account_service=self.account_service,
            customer_service=self.customer_service,
            inquiry_service=self.inquiry_service,
            query_repository=self.query_repository,
            config=self.config,
            email_service=self.email_service,
            job_control_service=self.job_control_service
        )
        
        sdg_result = sdg_job.execute(region=region, count=sdg_count)
        
        # Combine results
        created_accounts = mining_result.get("created_accounts", []) + sdg_result.get("created_accounts", [])
        success_count = mining_result.get("created_count", 0) + sdg_result.get("created_count", 0)
        failure_count = mining_result.get("failed_count", 0) + sdg_result.get("failed_count", 0)
        
        # Update job summary
        self.summary["success_count"] = success_count
        self.summary["failure_count"] = failure_count
        
        # Return combined results
        return {
            "threshold": threshold,
            "created_count": success_count,
            "failed_count": failure_count,
            "region": region,
            "mining_percent": mining_percent,
            "mining_result": {
                "count": mining_count,
                "created": mining_result.get("created_count", 0),
                "failed": mining_result.get("failed_count", 0)
            },
            "sdg_result": {
                "count": sdg_count,
                "created": sdg_result.get("created_count", 0),
                "failed": sdg_result.get("failed_count", 0)
            },
            "created_accounts": created_accounts
        }


class DDAThresholdBusinessJob(BaseJob):
    """
    Job for creating business DDA accounts up to a configured threshold.
    
    This job creates a mix of business accounts from both mining and SDG sources
    up to a specified threshold.
    """
    
    def __init__(self, account_service, customer_service, inquiry_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize DDA Threshold Business job."""
        super().__init__("dda_threshold_b", account_service.logger, config, email_service)
        
        # Store dependencies
        self.account_service = account_service
        self.customer_service = customer_service
        self.inquiry_service = inquiry_service
        self.query_repository = query_repository
        self.job_control_service = job_control_service
    
    def _execute(self, region: str = "SIT1", threshold: int = 50, mining_percent: int = 50, **kwargs) -> Dict[str, Any]:
        """
        Execute the DDA threshold job for business accounts.
        
        Args:
            region: Target region
            threshold: Maximum number of accounts to create
            mining_percent: Percentage of accounts to create from mining source (0-100)
            **kwargs: Additional parameters
            
        Returns:
            Dict containing job execution results
        """
        # Check if job is enabled
        if self.job_control_service and not self.job_control_service.is_job_enabled(self.name, region):
            self.logger.log(f"Job {self.name} is disabled for region {region}")
            return {
                "status": "skipped",
                "message": f"Job {self.name} is disabled for region {region}",
                "created_count": 0,
                "region": region
            }
        
        self.logger.log(f"Creating business DDA accounts up to threshold {threshold} in region {region}")
        
        # Calculate counts for each source
        mining_count = int(threshold * mining_percent / 100)
        sdg_count = threshold - mining_count
        
        # Create mining accounts
        mining_job = DDAMiningBusinessJob(
            account_service=self.account_service,
            customer_service=self.customer_service,
            inquiry_service=self.inquiry_service,
            query_repository=self.query_repository,
            config=self.config,
            email_service=self.email_service,
            job_control_service=self.job_control_service
        )
        
        mining_result = mining_job.execute(region=region, count=mining_count)
        
        # Create SDG accounts
        sdg_job = DDASDGBusinessJob(
            account_service=self.account_service,
            customer_service=self.customer_service,
            inquiry_service=self.inquiry_service,
            query_repository=self.query_repository,
            config=self.config,
            email_service=self.email_service,
            job_control_service=self.job_control_service
        )
        
        sdg_result = sdg_job.execute(region=region, count=sdg_count)
        
        # Combine results
        created_accounts = mining_result.get("created_accounts", []) + sdg_result.get("created_accounts", [])
        success_count = mining_result.get("created_count", 0) + sdg_result.get("created_count", 0)
        failure_count = mining_result.get("failed_count", 0) + sdg_result.get("failed_count", 0)
        
        # Update job summary
        self.summary["success_count"] = success_count
        self.summary["failure_count"] = failure_count
        
        # Return combined results
        return {
            "threshold": threshold,
            "created_count": success_count,
            "failed_count": failure_count,
            "region": region,
            "mining_percent": mining_percent,
            "mining_result": {
                "count": mining_count,
                "created": mining_result.get("created_count", 0),
                "failed": mining_result.get("failed_count", 0)
            },
            "sdg_result": {
                "count": sdg_count,
                "created": sdg_result.get("created_count", 0),
                "failed": sdg_result.get("failed_count", 0)
            },
            "created_accounts": created_accounts
        }



# app/account_automation/jobs/cca_job.py
"""
Job implementations for CCA account creation operations.
"""
import json
import random
from typing import Dict, Any, List, Optional
from app.account_automation.jobs.base_job import BaseJob


class CCAJob(BaseJob):
    """
    Base class for CCA (Credit Card Account) creation jobs.
    
    This class provides common functionality for CCA account creation
    jobs, regardless of the data source or customer type.
    """
    
    def __init__(self, name, account_service, customer_service, 
                query_repository, config=None, email_service=None, 
                job_control_service=None, customer_type="P", data_source="Mining"):
        """
        Initialize CCA job base class.
        
        Args:
            name: Job name
            account_service: Account service for account operations
            customer_service: Customer service for customer operations
            query_repository: Repository for SQL queries
            config: Optional configuration instance
            email_service: Optional email service for notifications
            job_control_service: Optional job control service
            customer_type: Customer type (P for Personal, B for Business)
            data_source: Data source (Mining or SDG)
        """
        super().__init__(name, account_service.logger, config, email_service)
        
        # Store dependencies
        self.account_service = account_service
        self.customer_service = customer_service
        self.query_repository = query_repository
        self.job_control_service = job_control_service
        
        # Store customer type and data source
        self.customer_type = customer_type
        self.data_source = data_source
        
        # Load signal data from database if available
        self._load_signal_data()
    
    def _load_signal_data(self) -> None:
        """
        Load signal data from database.
        
        This method loads configuration from the AutoSignalData table.
        """
        try:
            db = self.account_service.db
            query = self.query_repository.get_query('get_signal_data')
            
            # Determine signal data key based on region
            signal_key = 'RES Create CCA'
            
            signal_data = db.query_single(db.execute_param(query, [signal_key, 'RES']))
            
            if signal_data and hasattr(signal_data, 'SValue'):
                try:
                    # Parse JSON data
                    self.signal_data = json.loads(signal_data.SValue)
                    
                    # Log successful load
                    self.logger.log(f"Loaded signal data for {signal_key}")
                except json.JSONDecodeError as e:
                    self.logger.error(f"Error parsing signal data: {str(e)}")
                    self.signal_data = {}
            else:
                self.logger.warning(f"No signal data found for {signal_key}")
                self.signal_data = {}
        except Exception as e:
            self.logger.error(f"Error loading signal data: {str(e)}")
            self.signal_data = {}
    
    def get_threshold(self, region: str) -> int:
        """
        Get threshold for account creation based on configuration.
        
        Args:
            region: Target region
            
        Returns:
            Number of accounts to create
        """
        # Use account service to determine threshold
        return self.account_service.get_threshold(
            account_type="cca",
            customer_type=self.customer_type,
            region=region,
            datasource=self.data_source
        )
    
    def _execute(self, region: str = "SIT1", count: int = None, **kwargs) -> Dict[str, Any]:
        """
        Execute the CCA job.
        
        Args:
            region: Target region
            count: Number of accounts to create (overrides threshold if provided)
            **kwargs: Additional parameters
            
        Returns:
            Dict containing job execution results
        """
        # Check if job is enabled via job control service
        if self.job_control_service and not self.job_control_service.is_job_enabled(self.name, region):
            self.logger.log(f"Job {self.name} is disabled for region {region}")
            return {
                "status": "skipped",
                "message": f"Job {self.name} is disabled for region {region}",
                "created_count": 0,
                "region": region
            }
        
        # Determine number of accounts to create
        accounts_to_create = count if count is not None else self.get_threshold(region)
        
        self.logger.log(f"Creating {accounts_to_create} {self.customer_type} CCA accounts from {self.data_source} in region {region}")
        
        # No accounts to create
        if accounts_to_create <= 0:
            self.logger.log(f"No {self.customer_type} CCA accounts need to be created from {self.data_source} in {region}")
            return {
                "created_count": 0,
                "failed_count": 0,
                "region": region,
                "customer_type": self.customer_type,
                "data_source": self.data_source,
                "created_accounts": []
            }
        
        # Create accounts
        created_accounts = []
        success_count = 0
        failure_count = 0
        
        for i in range(accounts_to_create):
            try:
                self.logger.log(f"Creating {self.customer_type} CCA account {i+1}/{accounts_to_create} from {self.data_source}")
                
                # Create the account based on data source
                if self.data_source == "Mining":
                    # For mining, we pass empty string to use mining source
                    account_number = self.account_service.create_cca_mining("", region)
                    created_accounts.append({
                        "account_number": account_number,
                        "source": "Mining",
                        "type": "Personal" if self.customer_type == "P" else "Business"
                    })
                else:  # SDG
                    result = self.account_service.create_cca_synthetic(self.customer_type, region)
                    account_number = result["Account"]["AccountNumber"]
                    card_type = result["Account"].get("CardType", "")
                    
                    created_accounts.append({
                        "account_number": account_number,
                        "source": "SDG",
                        "type": "Personal" if self.customer_type == "P" else "Business",
                        "card_type": card_type
                    })
                
                self.logger.log(f"Successfully created CCA account: {account_number}")
                success_count += 1
                
            except Exception as e:
                self.logger.error(f"Failed to create {self.customer_type} CCA account from {self.data_source}: {str(e)}")
                failure_count += 1
        
        # Update job summary
        self.summary["success_count"] = success_count
        self.summary["failure_count"] = failure_count
        
        # Return detailed results
        return {
            "count": accounts_to_create,
            "created_count": success_count,
            "failed_count": failure_count,
            "region": region,
            "customer_type": self.customer_type,
            "data_source": self.data_source,
            "created_accounts": created_accounts
        }


class CCAMiningJob(CCAJob):
    """Job for creating CCA accounts from mining source."""
    
    def __init__(self, account_service, customer_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize CCA Mining job."""
        super().__init__(
            name="cca_mining",
            account_service=account_service,
            customer_service=customer_service,
            query_repository=query_repository,
            config=config,
            email_service=email_service,
            job_control_service=job_control_service,
            customer_type="P",  # Not relevant for Mining but needed for inheritance
            data_source="Mining"
        )


class CCASDGPersonalJob(CCAJob):
    """Job for creating personal CCA accounts from SDG source."""
    
    def __init__(self, account_service, customer_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize CCA SDG Personal job."""
        super().__init__(
            name="cca_sdg_p",
            account_service=account_service,
            customer_service=customer_service,
            query_repository=query_repository,
            config=config,
            email_service=email_service,
            job_control_service=job_control_service,
            customer_type="P",
            data_source="SDG"
        )


class CCASDGBusinessJob(CCAJob):
    """Job for creating business CCA accounts from SDG source."""
    
    def __init__(self, account_service, customer_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize CCA SDG Business job."""
        super().__init__(
            name="cca_sdg_b",
            account_service=account_service,
            customer_service=customer_service,
            query_repository=query_repository,
            config=config,
            email_service=email_service,
            job_control_service=job_control_service,
            customer_type="B",
            data_source="SDG"
        )


class CCAThresholdPersonalJob(BaseJob):
    """
    Job for creating personal CCA accounts up to a configured threshold.
    
    This job creates a mix of personal accounts from both mining and SDG sources
    up to a specified threshold.
    """
    
    def __init__(self, account_service, customer_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize CCA Threshold Personal job."""
        super().__init__("cca_threshold_p", account_service.logger, config, email_service)
        
        # Store dependencies
        self.account_service = account_service
        self.customer_service = customer_service
        self.query_repository = query_repository
        self.job_control_service = job_control_service
    
    def _execute(self, region: str = "SIT1", threshold: int = 50, mining_percent: int = 50, **kwargs) -> Dict[str, Any]:
        """
        Execute the CCA threshold job for personal accounts.
        
        Args:
            region: Target region
            threshold: Maximum number of accounts to create
            mining_percent: Percentage of accounts to create from mining source (0-100)
            **kwargs: Additional parameters
            
        Returns:
            Dict containing job execution results
        """
        # Check if job is enabled
        if self.job_control_service and not self.job_control_service.is_job_enabled(self.name, region):
            self.logger.log(f"Job {self.name} is disabled for region {region}")
            return {
                "status": "skipped",
                "message": f"Job {self.name} is disabled for region {region}",
                "created_count": 0,
                "region": region
            }
        
        self.logger.log(f"Creating personal CCA accounts up to threshold {threshold} in region {region}")
        
        # Calculate counts for each source
        mining_count = int(threshold * mining_percent / 100)
        sdg_count = threshold - mining_count
        
        # Create mining accounts
        mining_job = CCAMiningJob(
            account_service=self.account_service,
            customer_service=self.customer_service,
            query_repository=self.query_repository,
            config=self.config,
            email_service=self.email_service,
            job_control_service=self.job_control_service
        )
        
        mining_result = mining_job.execute(region=region, count=mining_count)
        
        # Create SDG accounts
        sdg_job = CCASDGPersonalJob(
            account_service=self.account_service,
            customer_service=self.customer_service,
            query_repository=self.query_repository,
            config=self.config,
            email_service=self.email_service,
            job_control_service=self.job_control_service
        )
        
        sdg_result = sdg_job.execute(region=region, count=sdg_count)
        
        # Combine results
        created_accounts = mining_result.get("created_accounts", []) + sdg_result.get("created_accounts", [])
        success_count = mining_result.get("created_count", 0) + sdg_result.get("created_count", 0)
        failure_count = mining_result.get("failed_count", 0) + sdg_result.get("failed_count", 0)
        
        # Update job summary
        self.summary["success_count"] = success_count
        self.summary["failure_count"] = failure_count
        
        # Return combined results
        return {
            "threshold": threshold,
            "created_count": success_count,
            "failed_count": failure_count,
            "region": region,
            "mining_percent": mining_percent,
            "mining_result": {
                "count": mining_count,
                "created": mining_result.get("created_count", 0),
                "failed": mining_result.get("failed_count", 0)
            },
            "sdg_result": {
                "count": sdg_count,
                "created": sdg_result.get("created_count", 0),
                "failed": sdg_result.get("failed_count", 0)
            },
            "created_accounts": created_accounts
        }


class CCAThresholdBusinessJob(BaseJob):
    """
    Job for creating business CCA accounts up to a configured threshold.
    
    This job creates a mix of business accounts from both mining and SDG sources
    up to a specified threshold.
    """
    
    def __init__(self, account_service, customer_service, 
                query_repository, config=None, email_service=None, job_control_service=None):
        """Initialize CCA Threshold Business job."""
        super().__init__("cca_threshold_b", account_service.logger, config, email_service)
        
        # Store dependencies
        self.account_service = account_service
        self.customer_service = customer_service
        self.query_repository = query_repository
        self.job_control_service = job_control_service
    
    def _execute(self, region: str = "SIT1", threshold: int = 30, mining_percent: int = 50, **kwargs) -> Dict[str, Any]:
        """
        Execute the CCA threshold job for business accounts.
        
        Args:
            region: Target region
            threshold: Maximum number of accounts to create
            mining_percent: Percentage of accounts to create from mining source (0-100)
            **kwargs: Additional parameters
            
        Returns:
            Dict containing job execution results
        """
        # Check if job is enabled
        if self.job_control_service and not self.job_control_service.is_job_enabled(self.name, region):
            self.logger.log(f"Job {self.name} is disabled for region {region}")
            return {
                "status": "skipped",
                "message": f"Job {self.name} is disabled for region {region}",
                "created_count": 0,
                "region": region
            }
        
        self.logger.log(f"Creating business CCA accounts up to threshold {threshold} in region {region}")
        
        # Calculate counts for each source
        mining_count = int(threshold * mining_percent / 100)
        sdg_count = threshold - mining_count
        
        # Create mining accounts
        mining_job = CCAMiningJob(
            account_service=self.account_service,
            customer_service=self.customer_service,
            query_repository=self.query_repository,
            config=self.config,
            email_service=self.email_service,
            job_control_service=self.job_control_service
        )
        
        mining_result = mining_job.execute(region=region, count=mining_count)
        
        # Create SDG accounts
        sdg_job = CCASDGBusinessJob(
            account_service=self.account_service,
            customer_service=self.customer_service,
            query_repository=self.query_repository,
            config=self.config,
            email_service=self.email_service,
            job_control_service=self.job_control_service
        )
        
        sdg_result = sdg_job.execute(region=region, count=sdg_count)
        
        # Combine results
        created_accounts = mining_result.get("created_accounts", []) + sdg_result.get("created_accounts", [])
        success_count = mining_result.get("created_count", 0) + sdg_result.get("created_count", 0)
        failure_count = mining_result.get("failed_count", 0) + sdg_result.get("failed_count", 0)
        
        # Update job summary
        self.summary["success_count"] = success_count
        self.summary["failure_count"] = failure_count
        
        # Return combined results
        return {
            "threshold": threshold,
            "created_count": success_count,
            "failed_count": failure_count,
            "region": region,
            "mining_percent": mining_percent,
            "mining_result": {
                "count": mining_count,
                "created": mining_result.get("created_count", 0),
                "failed": mining_result.get("failed_count", 0)
            },
            "sdg_result": {
                "count": sdg_count,
                "created": sdg_result.get("created_count", 0),
                "failed": sdg_result.get("failed_count", 0)
            },
            "created_accounts": created_accounts
        }
