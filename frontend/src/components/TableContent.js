const TableContent = (props) => {
    const { tableData } = props;
    return (
        <div className="data-table-container">
            <div className="table-header">
                <h4>{tableData.title}</h4>
            </div>
            <div className="table-wrapper">
                <table className="data-table">
                <thead>
                    <tr>
                    {tableData.headers.map((header, index) => (
                        <th key={index}>{header}</th>
                    ))}
                    </tr>
                </thead>
                <tbody>
                    {tableData.rows.map((row, rowIndex) => (
                    <tr key={rowIndex} className={row[0] === 'TOTAL' ? 'total-row' : ''}>
                        {row.map((cell, cellIndex) => (
                        <td key={cellIndex} className={cellIndex === 0 ? 'first-col' : ''}>{cell}</td>
                        ))}
                    </tr>
                    ))}
                </tbody>
                </table>
            </div>
        </div>
    );
};
export default TableContent;
