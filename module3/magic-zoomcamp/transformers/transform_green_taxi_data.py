if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    
    print('The rows with both zero passenger count and trip distance: ',data[(data['passenger_count'] == 0) & (data['trip_distance'] == 0)].shape[0])

    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    data = data.rename(columns={'VendorID' : 'vendor_id', 'RatecodeID' : 'ratecode_id',
    'PULocationID' : 'pu_location_id', 'DOLocationID' : 'do_location_id'})

    #print(data['vendor_id'].unique().dropna())
    #print(len(data['lpep_pickup_date'].unique()))

    return data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]


@test
def test_output_columns(output, *args) -> None:

    assert 'vendor_id' in output.columns, 'The mentioned column does not exist among the columns.'


@test
def test_passenger_count(output, *args) -> None:

    # OR assert output[output['passenger_count'] == 0].shape[0] == 0, 'There are rides with zero passengers.'
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers.'


@test
def test_trip_distance(output, *args) -> None:

    # OR assert output[output['trip_distance'] == 0].shape[0] == 0, 'There are rides with zero trip distance.'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero trip distance.'