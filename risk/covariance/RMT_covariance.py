from qfengine.risk.covariance.covariance import CovarianceMatrixRiskModel
import numpy as np
import pandas as pd

class RMTCovarianceRiskModel(CovarianceMatrixRiskModel):
    '''
    Random Matrix Theory (RMT) Implementation of Marchenko-Pastur Filtering
    of noisy eigen values
    '''

    def __init__(self,
                 universe,
                 data_handler,
                    Q = None,
                    sigma = None,
                **kwargs
    ):
        def RMTFilteredCorrelation(ret, Q = Q, sigma = sigma):
            T,N = ret.shape
            Q = Q if Q is not None else (T/N) #---| optimizable
            sigma = sigma if sigma is not None else  1 #---| optimizable

            #! Marchenko-Pastur Theoretical Range Equation
            min_theoretical_eval, max_theoretical_eval = (
                                            np.power(sigma*(1 - np.sqrt(1/Q)),2),
                                            np.power(sigma*(1 + np.sqrt(1/Q)),2)
                                                         )
            raw_corr = ret.corr()
            eVals,eVecs = np.linalg.eigh(raw_corr.values)
            # noise_eVals = eVals[eVals <= max_theoretical_eval]
            # outlier_eVals = eVals[eVals > max_theoretical_eval]
            #---| Filter eigen values by replacing those in theoretical range to 0 (noises)
            filtered_eVals = [(0 if ((i >= min_theoretical_eval) and (i<= max_theoretical_eval)) else i) for i in eVals]
            #-----| Part 2b: Construct Filtered Correlation Matrix from Filtered eVals
            filtered_corr = np.dot(eVecs,np.dot(
                                np.diag(filtered_eVals),np.transpose(eVecs)
                                                ))
            np.fill_diagonal(filtered_corr,1)
            return pd.DataFrame(data=filtered_corr,index=raw_corr.index,columns=raw_corr.columns)
        
        super().__init__(universe = universe,
                         data_handler = data_handler,
                         ret_corr_op= RMTFilteredCorrelation,
                         **kwargs
                         )

