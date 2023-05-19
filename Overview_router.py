from imports import *
# from Overview_data.Cex import data
from Overview_data.Cex import data
from Overview_data.Dexx import DAI_pie_df, LUSD_pie, Tusd_pie, DAI, LUSD, TUSD
from Overview_data.Dexx import *


overview_router = APIRouter(
    prefix='/overview',
    tags=['overview']
)


@overview_router.get('/Cex')
async def choice_time(start: str, end: str, label: str):
    labels = data[data['Symbols'] == label]
    if labels.empty:
        return {'status': 'fail', 'message': f'Label "{label}" not found.'}
    else:
        data_json = data[data['TimeStamp'].between(start, end)]
        data_json = data_json[data_json['Symbols'] == label]
        return data_json.to_dict(orient='records')


@overview_router.get('/Cex/pie')
async def pie_day():
    pie_df = data[data['TimeStamp'] == data['TimeStamp'].max()]
    pie_df = pie_df.sort_values(by='SUM', ascending=False)
    others = pie_df[4:]
    others = pd.DataFrame({
        'TimeStamp': others['TimeStamp'].unique(),
        'Symbols': ['Others'],
        'SUM': others['SUM'].sum()
    })
    create_df = pd.concat([pie_df, others], ignore_index=True)
    create_df = create_df.drop(
        create_df[create_df['SUM'] == 0.].index)
    return create_df.to_dict(orient='records')

# DEX


@overview_router.get('/Dex/pie')
async def pie_date(label: str):
    if label == 'Dai':
        return DAI_pie_df.to_dict(orient='records')
    elif label == 'Lusd':
        return LUSD_pie.to_dict(orient='records')
    elif label == 'Tusd':
        return Tusd_pie.to_dict(orient='records')
    else:
        return {'status': 'fail', 'message': f'Label "{label}" not found.'}


@overview_router.get('/Dex/')
async def choice_time(start: str, end: str, label: str):
    if label == 'Lusd':
        LUSD_line = lusd_line(LUSD)
        return LUSD_line.to_dict(orient='records')
    elif label == 'Dai':
        dai_df = Dai_line(DAI)
        return dai_df.to_dict(orient='records')
    elif label == 'Tusd':
        tusd_df = Tusd_line(TUSD)
        return tusd_df.to_dict(orient='records')

    else:
        return {'status': 'fail', 'message': f'Label "{label}" not found.'}


# Bridge Overviews
@overview_router.get('/Bridge/pie')
async def Bridge_pie():
    return {'message': 'Hello this is awaiting for overview'}


# đoạn này sẽ có input ;{start}{end}{Bridge_name}:
@overview_router.get('/Bridge/')
async def choice_bridge():
    return {'message': 'Choose Bridge you one'}
