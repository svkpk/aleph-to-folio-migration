# ------------------------------------------------------------------------------
#   Obecne
# ------------------------------------------------------------------------------
OKAPI_URL = 'http://FOLIO_HOSTNAME:9130'

FOLIO_TENANT = 'tenant'
FOLIO_USERNAME = 'username'
FOLIO_USERPASS = 'pass'

OAI_url = 'https://aleph22.svkpk.cz/OAI'
OAI_set = 'PNA01-NEWILS-MARC21'
OAI_meta_prefix = 'marc21'

# UUID identifikator zaznamu instituce
# Settings -> Tenant -> Location setup -> Institutions
institutionId = '40ee00ca-a518-4b49-be01-0638d0a4ac57'

# UUID identifikator zaznamu kampusu pod kterym se bude pomoci scriptu 
# tenant_locations_setup.py vytvaret 
# Settings -> Tenant -> Location setup -> Campuses
campusId = '62cf76b7-cca5-4d33-9217-edf42ce1a848'

# UUID identifikator zaznamu spravce poplatku
# Settings -> Users -> Fee/fine -> Owners
ownerId = '0468181d-2f41-4f94-b2b2-ffadb410431b'


# ------------------------------------------------------------------------------
#   OKAPI authentizace
# ------------------------------------------------------------------------------
OKAPI_LOGIN_URL = OKAPI_URL + '/authn/login'

# ------------------------------------------------------------------------------
#   tenant_location_setup.py
# ------------------------------------------------------------------------------
OKAPI_LIBRARIES_URL = OKAPI_URL + '/location-units/libraries'
OKAPI_LOCATIONS = OKAPI_URL + '/locations'
fn_okapi_token = 'working_dir/okapi_token'
fn_libraries_uuid = 'working_dir/libraries_uuid.list'
fn_locations_uuid = 'working_dir/locations_uuid.list'
fn_locations_tsv = 'working_dir/locations.tsv'
servicePointList = ['3a40852d-49fd-4df2-a1f9-6e2641a6e91f']
primaryServicePoint = '3a40852d-49fd-4df2-a1f9-6e2641a6e91f'

# ------------------------------------------------------------------------------
#   loan types = z30_item_status
# ------------------------------------------------------------------------------
OKAPI_LOAN_TYPES = OKAPI_URL + '/loan-types'
fn_loan_types_mapping = 'mapping_files/loan_types_csv.tsv'
fn_loan_types_uuid = 'working_dir/loan_types_uuid.list'

# ------------------------------------------------------------------------------
#   material types = z30_material_type
# ------------------------------------------------------------------------------
OKAPI_MATERIAL_TYPES = OKAPI_URL + '/material-types'
fn_material_types_mapping = 'mapping_files/material_types_csv.tsv'
fn_material_types_uuid = 'working_dir/material_types_uuid.list'

# ------------------------------------------------------------------------------
#   user groups = Z305_BOR_STATUS
# ------------------------------------------------------------------------------
OKAPI_USER_GROUPS = OKAPI_URL + '/groups'
fn_user_groups_mapping = 'mapping_files/user_groups.tsv'
fn_user_groups_uuid = 'working_dir/user_groups_uuid.list'

# ------------------------------------------------------------------------------
#   address types = Z304_ADDRESS_TYPE
# ------------------------------------------------------------------------------
OKAPI_ADDR_TYPES = OKAPI_URL + '/addresstypes'
fn_addr_types_mapping = 'mapping_files/addr_types.tsv'
fn_addr_types_uuid = 'working_dir/addr_types_uuid.list'

# ------------------------------------------------------------------------------
#   fee/fine types = Z31_TYPE
# ------------------------------------------------------------------------------
OKAPI_FEEFINE_TYPES = OKAPI_URL + '/feefines'
fn_feefine_types_mapping = 'mapping_files/feefine_types.tsv'
fn_feefine_types_uuid = 'working_dir/feefine_types_uuid.list'

# ------------------------------------------------------------------------------
#   service points = Z37_PICKUP_LOCATION
# ------------------------------------------------------------------------------
OKAPI_SERVICE_POINTS = OKAPI_URL + '/service-points'
fn_service_points_mapping = 'mapping_files/service_points.tsv'
fn_service_points_uuid = 'working_dir/service_points_uuid.list'

# ------------------------------------------------------------------------------
#   user csv data from Aleph
# ------------------------------------------------------------------------------
fn_user_z303 = 'aleph_data_example_z303.csv'
fn_user_z304 = 'aleph_data_example_z304.csv'
fn_user_z305 = 'aleph_data_example_z305.csv'
fn_user_z308 = 'aleph_data_example_z308.csv'

# ------------------------------------------------------------------------------
#   mapping db user_id -> barcode
# ------------------------------------------------------------------------------
fn_userid_barcode_db = 'working_dir/userid_barcode_db'
# ------------------------------------------------------------------------------
#   mapping db item_id -> barcode
# ------------------------------------------------------------------------------
fn_itemid_barcode_db = 'working_dir/itemid_barcode_db'
