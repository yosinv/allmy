from inference_schema.schema_decorators import input_schema, output_schema
from inference_schema.parameter_types.standard_py_parameter_type import StandardPythonParameterType
import json


input_sample = json.dumps([{
    "Claim_Nr": 1350302,
    "Client_Claim": "034124149",
    "CLAIM_DATE": 1556035200000,
    "Claim_Place_Code": "4",
    "Claim_Place_Zone_Code": "\\u05d1\\u05d0\\u05e8",
    "Fualt_Insured_Code": 4.0,
    "Vehicle_Towed": 0.0,
    "Police_arrive": "nodata",
    "Photos_taken": "nodata"
}, ])


output_sample = [
    {
        "Client": 208140,
        "Policy_Nr": 1127397702,
        "Endors_Nr": 1,
        "proba": 0.305826,
        "Lima_prediction_id": 2,
        "Lima_prediction": "ללא העדפה"
    }, ]

standard_sample_input = {'name': ['Sarah', 'John'], 'age': [25, 26]}
standard_sample_output = {'age': [25, 26]}


@input_schema('param', StandardPythonParameterType(standard_sample_input))
@output_schema(StandardPythonParameterType(standard_sample_output))
def run(param):
    try:
        result = param
        #     return result.tolist()
        print('predict')
        return result

    except Exception as e:
        error = str(e)
        print('error')
        return error

run(standard_sample_output)

