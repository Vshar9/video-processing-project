package com.vflow.metadata_service.repository;

import com.vflow.metadata_service.model.Video;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface VideoRepository extends JpaRepository<Video,Long>{
    
}