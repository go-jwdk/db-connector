package dbconnector

import (
	"context"
)

type grabberMock struct {
	GrabJobsFunc func(ctx context.Context, input *GrabJobsInput) (*GrabJobsOutput, error)
}

func (m *grabberMock) GrabJobs(ctx context.Context, input *GrabJobsInput) (*GrabJobsOutput, error) {
	if m.GrabJobsFunc == nil {
		panic("This method is not defined.")
	}
	return m.GrabJobsFunc(ctx, input)
}
