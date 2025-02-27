package com.docde.domain.search.controller;

import com.docde.common.response.ApiResponse;
import com.docde.domain.hospital.entity.Hospital;
import com.docde.domain.hospital.entity.HospitalDocument;
import com.docde.domain.search.dto.SearchResponse;
import com.docde.domain.search.service.SearchService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class SearchController {
    private final SearchService searchService;

    @GetMapping("/hospitals/legacy/search")
    public ResponseEntity<ApiResponse<Page<SearchResponse.SearchHospital>>> searchHospitalLegacy(@RequestParam("q") String query, @RequestParam(value = "page", defaultValue = "0") Integer page, @RequestParam(value = "size", defaultValue = "10") Integer size) {
        Page<Hospital> hospitals = searchService.searchHospitalLegacy(query, page, size);
        return ApiResponse.onSuccess(hospitals.map(v -> new SearchResponse.SearchHospital(v.getId(), v.getName(), v.getAddress(), v.getContact(), v.getOpenTime(), v.getClosingTime()))).toEntity();
    }

    @GetMapping("/hospitals/search")
    public ResponseEntity<ApiResponse<Page<HospitalDocument>>> searchHospital(@RequestParam("q") String query, @RequestParam(value = "page", defaultValue = "0") Integer page, @RequestParam(value = "size", defaultValue = "10") Integer size) {
        return ApiResponse.onSuccess(searchService.searchHospital(query, page, size)).toEntity();
    }
}
