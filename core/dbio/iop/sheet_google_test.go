package iop

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

type sheetTestTransport func(*http.Request) (*http.Response, error)

func (f sheetTestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestGoogleSheetWriteSheet(t *testing.T) {
	for _, tc := range []struct {
		mode string
		cell string
		want [][]any
	}{
		{
			mode: "overwrite",
			cell: "A1",
			want: [][]any{{"Name", ""}, {"New name", ""}, {"", "", ""}},
		},
		{
			mode: "append",
			cell: "A4",
			want: [][]any{{"New name"}},
		},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			var update sheets.ValueRange
			client := &http.Client{Transport: sheetTestTransport(func(req *http.Request) (*http.Response, error) {
				body := `{"sheets":[{"properties":{"title":"People"}}]}`
				switch req.Method {
				case http.MethodPut:
					require.Equal(t, "/v4/spreadsheets/test/values/People!"+tc.cell, req.URL.Path)
					require.NoError(t, json.NewDecoder(req.Body).Decode(&update))
					body = "{}"
				case http.MethodGet:
					require.Equal(t, "/v4/spreadsheets/test", req.URL.Path)
				default:
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL)
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}
			service, err := sheets.NewService(context.Background(), option.WithHTTPClient(client))
			require.NoError(t, err)

			grid := &sheets.GridData{}
			for _, values := range [][]string{{"Name", "Old heading"}, {"Old name", "Old value"}, {"Other name", "", "Extra value"}} {
				row := &sheets.RowData{}
				for _, value := range values {
					row.Values = append(row.Values, &sheets.CellData{FormattedValue: value})
				}
				grid.RowData = append(grid.RowData, row)
			}
			sheet := &GoogleSheet{
				srv:           service,
				SpreadsheetID: "test",
				sheetObjects:  map[string]*sheets.Sheet{"People": {Data: []*sheets.GridData{grid}}},
			}

			data := NewDataset(Columns{{Name: "Name", Type: StringType}})
			data.Append([]any{"New name"})

			require.NoError(t, sheet.WriteSheet("People", data.Stream(), tc.mode))
			require.Equal(t, tc.want, update.Values)
		})
	}
}
