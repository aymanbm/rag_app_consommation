import TableContent from "./TableContent";

const Response = ({ responseData, tableData }) => {
  return (
    <>
      {responseData.error ? (
        <div className="error-message">
          <svg
            xmlns="http://www.w3.org/2000/svg"
            width="20"
            height="20"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <circle cx="12" cy="12" r="10" />
            <line x1="15" y1="9" x2="9" y2="15" />
            <line x1="9" y1="9" x2="15" y2="15" />
          </svg>
          {responseData?.error}
        </div>
      ) : (
        <>
          {/* Natural Language Response */}
          {(responseData.response || responseData.llm_response) && (
            <div className="natural-response">
              <div className="response-header">
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  width="18"
                  height="18"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                >
                  <path d="M12 8V4H8"></path>
                  <rect x="4" y="4" width="16" height="16" rx="2"></rect>
                  <path d="M2 14h2"></path>
                  <path d="M20 14h2"></path>
                  <path d="M15 13v2"></path>
                  <path d="M9 13v2"></path>
                </svg>
                <span>Réponse IA</span>
              </div>
              <p className="response-text">
                {responseData.response || responseData.llm_response}
              </p>
            </div>
          )}

          {/* Dynamic Data Table */}
          {tableData && tableData.rows.length > 0 && (
            <TableContent tableData={tableData} />
          )}
        </>
      )}
    </>
  );
}; 

export default Response;
