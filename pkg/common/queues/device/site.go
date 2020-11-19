package device

// Equals returns whether the input Site is equal
func (s Site) Equals(in Site) bool {
	return in.ID == s.ID && s.Name == in.Name && s.Country == in.Country
}
