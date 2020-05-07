package dbconnector

import "testing"

func TestQueueAttributes_HasDeadLetter(t *testing.T) {
	type fields struct {
		DeadLetterTarget *string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
		want1  bool
	}{
		{
			name: "has dead letter",
			fields: fields{
				DeadLetterTarget: func() *string { v := "foo"; return &v }(),
			},
			want:  "foo",
			want1: true,
		},
		{
			name: "has not dead letter",
			fields: fields{
				DeadLetterTarget: func() *string { v := ""; return &v }(),
			},
			want:  "",
			want1: false,
		},
		{
			name: "has not dead letter",
			fields: fields{
				DeadLetterTarget: nil,
			},
			want:  "",
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := QueueAttributes{
				DeadLetterTarget: tt.fields.DeadLetterTarget,
			}
			got, got1 := q.HasDeadLetter()
			if got != tt.want {
				t.Errorf("HasDeadLetter() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("HasDeadLetter() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
