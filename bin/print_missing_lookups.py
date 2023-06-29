import csv
import os
from pathlib import Path

from digital_land.phase.phase import Phase
from digital_land.phase.convert import ConvertPhase
from digital_land.phase.normalise import NormalisePhase
from digital_land.phase.parse import ParsePhase
from digital_land.phase.concat import ConcatFieldPhase
from digital_land.phase.map import MapPhase
from digital_land.phase.filter import FilterPhase
from digital_land.phase.patch import PatchPhase
from digital_land.phase.harmonise import HarmonisePhase
from digital_land.phase.default import DefaultPhase
from digital_land.phase.migrate import MigratePhase
from digital_land.phase.organisation import OrganisationPhase
from digital_land.phase.prune import FieldPrunePhase
from digital_land.phase.reference import EntityReferencePhase
from digital_land.phase.prefix import EntityPrefixPhase
from digital_land.pipeline import run_pipeline,Pipeline
from digital_land.specification import Specification
from digital_land.organisation import Organisation
from digital_land.collection import Collection

from digital_land.log import DatasetResourceLog,ColumnFieldLog,IssueLog


from digital_land.commands import resource_from_path

from digital_land.phase.lookup import key
lookup_file_path = './pipeline/lookup.csv' 
# sort lookups
# 44000000,44999999
# if os.path.exists('./pipeline/lookup.csv'):
#     lookups = []
#     entities = []
#     fieldnames = []
#     with open(lookup_file_path) as f:
#         dictreader = csv.DictReader(f)
#         fieldnames = dictreader.fieldnames
#         for row in dictreader:
#             lookups.append(row)
#             entities.append(int(row['entity']))

#     entities = list(dict.fromkeys(entities))
#     entities = sorted(entities)
#     entity_min = entities[0]
#     entity_max = entities[-1]
#     complete_entities = list(range(entity_min,entity_max))
#     entity_gaps = [e for e in complete_entities if e not in entities]
#     print(f' minimum entity: {entity_min}')
#     print(f' maximum entity: {entity_max}')
#     entity_gaps_string = ', '.join(str(entity_gaps))
#     print(f' entity number gaps: {entity_gaps}')

#     lookups = sorted(lookups,key=lambda d: d['entity'])
#     with open(lookup_file_path,'w') as f:
#         dictwriter = csv.DictWriter(f,fieldnames=fieldnames)
#         dictwriter.writeheader()
#         dictwriter.writerows(lookups)

# print all lookups that aren't found will need to read through all files
class PrintLookupPhase(Phase):
    def __init__(self,lookups={}):
        self.lookups = lookups
        self.entity_field = "entity"
    
    def lookup(self, **kwargs):
        return self.lookups.get(key(**kwargs), "")
    
    def process(self, stream):
        for block in stream:
            row = block["row"]
            entry_number = block["entry-number"]
            prefix = row.get("prefix", "")
            reference = row.get("reference", "")
            organisation = row.get("organisation", "")
            if prefix:
                if not row.get(self.entity_field, ""):
                    entity = (
                        # by the resource and row number
                        (
                            self.entity_field == "entity"
                            and self.lookup(prefix=prefix, entry_number=entry_number)
                        )
                        # TBD: fixup prefixes so this isn't needed ..
                        # or by the organisation and the reference
                        or self.lookup(
                            prefix=prefix,
                            organisation=organisation,
                            reference=reference,
                        )
                    )
            
            if not entity:
                print(f'{prefix},{organisation},{reference}')

            yield block


# run pipeline until look phase then create our own phase to collect lookups
def get_resource_unidentified_lookups(input_path,dataset,organisations):
    #  define pipeline and specification
    pipeline = Pipeline('./pipeline', dataset)
    specification = Specification('./specification')

    # convert phase inputs
    resource = resource_from_path(input_path)
    dataset_resource_log = DatasetResourceLog(dataset=dataset, resource=resource)
    custom_temp_dir='./var'

    # normalise phase inputs
    skip_patterns = pipeline.skip_patterns(resource)
    null_path = None

    # concatfieldphase 
    concats = pipeline.concatenations(resource)
    column_field_log = ColumnFieldLog(dataset=dataset, resource=resource)

    # map phase
    intermediate_fieldnames = specification.intermediate_fieldnames(pipeline)
    columns = pipeline.columns(resource)

    # patch phase
    patches = pipeline.patches(resource=resource)

    # harmonize phase
    issue_log = IssueLog(dataset=dataset, resource=resource)

    # default phase
    default_fields = pipeline.default_fields(resource=resource)
    default_values = pipeline.default_values(endpoints=[])

    if len(organisations) == 1:
        default_values["organisation"] = organisations[0]

    # migrate phase
    schema = specification.pipeline[pipeline.name]["schema"]

    # organisation phase
    organisation = Organisation('./var/cache/organisation.csv', Path(pipeline.path))

    # print lookups phase
    flookups = pipeline.lookups()


    run_pipeline(
            ConvertPhase(
                path=input_path,
                dataset_resource_log=dataset_resource_log,
                custom_temp_dir=custom_temp_dir,
            ),
            NormalisePhase(skip_patterns=skip_patterns, null_path=null_path),
            ParsePhase(),
            ConcatFieldPhase(concats=concats, log=column_field_log),
            MapPhase(
                fieldnames=intermediate_fieldnames,
                columns=columns,
                log=column_field_log,
            ),
            FilterPhase(filters=pipeline.filters(resource)),
            PatchPhase(
                issues=issue_log,
                patches=patches,
            ),
            HarmonisePhase(
                specification=specification,
                issues=issue_log,
            ),
            DefaultPhase(
                default_fields=default_fields,
                default_values=default_values,
                issues=issue_log,
            ),
            # TBD: move migrating columns to fields to be immediately after map
            # this will simplify harmonisation and remove intermediate_fieldnames
            # but effects brownfield-land and other pipelines which operate on columns
            MigratePhase(
                fields=specification.schema_field[schema],
                migrations=pipeline.migrations(),
            ),
            OrganisationPhase(organisation=organisation),
            FieldPrunePhase(fields=specification.current_fieldnames(schema)),
            EntityReferencePhase(
                dataset=dataset,
                specification=specification,
            ),
            EntityPrefixPhase(dataset=dataset),
            PrintLookupPhase(lookups=flookups)
    )


# run for resources and datasets
# collection = Collection(name=None, directory='./collection')
# collection.load()

# dataset_resource_map = collection.dataset_resource_map()
# for dataset in dataset_resource_map:
#     for resource in dataset_resource_map[dataset]:
#         print(resource)
#         get_resource_unidentified_lookups(f'./collection/resource/{resource}',dataset,collection.resource_organisations(resource))
        
# run for individual dataset and resource
collection = Collection(name=None, directory='./collection')
dataset = 'brownfield-land'
collection.load()
resource = '2148f0139bed0fea24c7e01ac24af143ad44b30c6bd9db34e033ba9f7e7130cd'

get_resource_unidentified_lookups(f'./collection/resource/{resource}',dataset,collection.resource_organisations(resource))