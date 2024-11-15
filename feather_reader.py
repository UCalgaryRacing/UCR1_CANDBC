
import cudf
import pandas as pd


pandasRegular = pd.read_csv("pandas.csv")
pandasFeather = pd.read_feather("pandas.feather")

print(f"pandas dataframe from csv size: {len(pandasRegular.index)}")
print(f"pandas dataframe from feather size: {len(pandasFeather.index)}")

cudaRegular = pd.read_csv("cudf.csv")
cudaFeather = pd.read_feather("cudf.feather")

print(f"cuda dataframe from csv size: {len(cudaRegular.index)}")
print(f"cuda dataframe from feather size: {len(cudaFeather.index)}")


cudaRegularcudf = cudf.read_csv("cudf.csv")
cudaFeathercudf = cudf.read_feather("cudf.feather")

print(f"cuda dataframe from csv size read with cuda: {len(cudaRegularcudf.index)}")
print(f"cuda dataframe from feather size read with cuda: {len(cudaFeathercudf.index)}")