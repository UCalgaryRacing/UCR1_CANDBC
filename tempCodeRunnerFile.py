.read_csv(output_csv_path)
    dataFrame = dataFrame.interpolate(method='linear', limit_direction='forward', axis=0)
    filled_data_path = os.path.join(
        f"{output_path}", f"Linear Interpolation"α
    )
    if not os.path.exists(filled_data_path):
        os.makedirs(filled_data_path, exist_ok=True)
    output_csv_path = os.path.join(
        f"{filled_data_path}", f"linear_{file_name}"
    )
    try:
        dataFrame.to_csv(output_csv_path, sep=',')
    except Exception as e:
        print("Unable to export {output_csv_path}")
    print(f"Interpolated decoded data has been exported to {output_csv_path}")