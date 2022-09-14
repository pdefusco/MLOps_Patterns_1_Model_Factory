# ###########################################################################
#
#  CLOUDERA APPLIED MACHINE LEARNING PROTOTYPE (AMP)
#  (C) Cloudera, Inc. 2022
#  All rights reserved.
#
#  Applicable Open Source License: Apache 2.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# ###########################################################################

import pandas as pd
import numpy as np
import onnxruntime
import onnxmltools
import onnx
import json, shutil, os
import cdsw

### Sample Input Data
#input_data = {
#  "acc_now_delinq": "4",
#  "acc_open_past_24mths": "329.08",
#  "annual_inc": "1",
#  "avg_cur_bal": "1",
#  "funded_amnt": "1"
#}

model_path = onnx.load("models/model.onnx").SerializeToString()

so = onnxruntime.SessionOptions()
so.add_session_config_entry('model.onnx', 'ONNX')

session = onnxruntime.InferenceSession(model_path)
output = session.get_outputs()[0] 
inputs = session.get_inputs()

@cdsw.model_metrics
def run(input_data):
    
    # Reformatting Input
    df = pd.DataFrame(input_data, index=[0])
    df.columns = ['acc_now_delinq', 'acc_open_past_24mths', 'annual_inc', 'avg_cur_bal', 'funded_amnt']
    df['acc_now_delinq'] = df['acc_now_delinq'].astype(float)
    df['acc_open_past_24mths'] = df['acc_open_past_24mths'].astype(float)
    df['annual_inc'] = df['annual_inc'].astype(float)
    df['avg_cur_bal'] = df['avg_cur_bal'].astype(float)
    df['funded_amnt'] = df['funded_amnt'].astype(float)
    
    # ONNX Scoring
    try:
        input_data= {i.name: v for i, v in zip(inputs, df.values.reshape(len(inputs),1,1).astype(np.float32))}
        output = session.run(None, input_data)
        pred = pd.DataFrame(output)[0][0]
        
        #cdsw.track_metric("input_data", input_data)
        cdsw.track_metric("prediction", pred)
        
        data = df.astype('str').to_dict('records')[0]
    
        # Track prediction
        cdsw.track_metric("prediction", str(pred))
    
        cdsw.track_metric("data", df.to_json())
    
        return {'input_data': str(data), 'prediction': str(pred)}
        

    except Exception as e:
        result_dict = {"error": str(e)}
    
    return result_dict
